import sys
import time
import argparse
import json
from itertools import islice, combinations
import pyspark
from pyspark import SparkContext
from operator import add
# from tqdm import tqdm

# def assertNotEqual(itemset1: tuple, itemset2: tuple, error_message: str = ""):
#     not_same_set = False
#     for i in range(len(itemset1)):
#         if itemset1[i] != itemset2[i]:
#             not_same_set = True
    
#     assert not_same_set, error_message

def mergeItemsets(itemset1: tuple, itemset2: tuple):
    
    result = []
    
    itemset1_i = 0
    itemset2_i = 0
    
    while itemset1_i < len(itemset1) and itemset2_i < len(itemset2):
        if itemset1[itemset1_i] < itemset2[itemset2_i]:
            result.append(itemset1[itemset1_i])
            itemset1_i += 1
        elif itemset2[itemset2_i] < itemset1[itemset1_i]:
            result.append(itemset2[itemset2_i])
            itemset2_i += 1
        else:
            itemset1_i += 1
    
    while itemset1_i < len(itemset1):
        if itemset1[itemset1_i] > result[-1]:
            result.append(itemset1[itemset1_i])
        itemset1_i += 1
        
    while itemset2_i < len(itemset2):
        if itemset2[itemset2_i] > result[-1]:
            result.append(itemset2[itemset2_i])
        itemset2_i += 1
    
    for i in range(1, len(result)):
        assert result[i] > result[i-1]
    
    return tuple(result)

# Function for phase 2 of the SON algorithm
def validateCandidates(basket_rdd, candidate_set, threshold):
    """
    Count occurrences of candidate itemsets in the data
    """
    return basket_rdd.flatMap(lambda entry: entry).filter(lambda entry: entry in candidate_set).map(lambda entry: (entry, 1)).reduceByKey(add).filter(lambda entry: entry[1] >= threshold).map(lambda entry: entry[0])
    

# Function for use in step 4 of aPriori algorithm
def getCandidatesForNextSize(iterator, candidate_set, size):
    """
    Generate candidate itemsets for the next iteration:
    1. Take frequent itemsets from the current iteration
    2. Join pairs of frequent itemsets to create larger candidates
    3. Apply the a-priori property to prune invalid candidates
    4. Return valid candidate itemsets of the specified size
    """
    
    # print(f"Itemset Size in Function: {size}")
    
    for basket in iterator:
        next_itemsets_in_basket = set()
        # if size == 3:
        #     print(basket)
        for i in range(len(basket)):
            for j in range(i+1, len(basket)):
                if basket[i] in candidate_set and basket[j] in candidate_set:
                    merged_itemset = mergeItemsets(basket[i], basket[j])
                    if len(merged_itemset) == len(basket[i])+1:
                        next_itemsets_in_basket.add(merged_itemset)
                    
        yield list(next_itemsets_in_basket)
                        

def aPriori(iterator, threshold, basket_count):
    """
    Implement the A-Priori algorithm:
    1. Process baskets to calculate partition-specific threshold
    2. Generate singleton itemsets from the data
    3. Identify frequent itemsets by counting and comparing to threshold
    4. Generate candidates for next iteration using frequent itemsets
    5. Validate each new set of candidates against the data
    6. Repeat until no new frequent itemsets are found
    7. Return the complete set of frequent itemsets
    """
    
    list_iterator = list(iterator)

    reduced_threshold = threshold*len(list_iterator)/basket_count
    
    counter = {}
    frequent_itemsets = set()
    
    for basket in list_iterator:
        for i in range(len(basket)):
            if basket[i] not in counter:
                counter[basket[i]] = 0
                
            counter[basket[i]] += 1
            
            if counter[basket[i]] >= reduced_threshold:
                frequent_itemsets.add(basket[i])
    
    yield frequent_itemsets
    
    
def transformCSVLine(csv_line: str, key_names: list[str]):
    if csv_line[1] == 0:
        return []

    nums = csv_line[0].split(',')
    
    transformation = {}
    
    for i in range(len(nums)):
        transformation[key_names[i]] = int(nums[i])
    
    return [transformation]

def is_subset(entry, candidate, needed_len):
    return len(set(entry).intersection(set(candidate))) == needed_len

# def partition_len(iterator):
#     yield (len(list(iterator)))
    
def main(rdd, case, threshold, outputJson, year_filter, hashSize=None):
    """
    Main function controlling the workflow and implementing the SON algo:
    1. Preprocess data (remove header, filter by year, ect)
    2. Convert data into baskets based on case
    3. Apply the SON algorithm with A-Priori
    4. Output results
    """
    out = {}
    '''
    
    YOUR CODE HERE
    
    '''
    # 1. Preprocess Data
    rdd_keys = rdd.first().split(',')
    
    transformed_rdd = rdd.zipWithIndex().flatMap(lambda entry: transformCSVLine(entry, rdd_keys)).filter(lambda entry: entry['year'] == year_filter and entry['rate'] != 0)
    
    # 2. Convert data into baskets
    if case == 1:
        basket_rdd = transformed_rdd.map(lambda entry: (str(entry['user_id']), (str(entry['business_id']),)))
    elif case == 2:
        basket_rdd = transformed_rdd.map(lambda entry: (str(entry['business_id']), (str(entry['user_id']),)))
    
    basket_rdd = basket_rdd.map(lambda entry: (entry[0], set([entry[1]]))).reduceByKey(lambda set1, set2: set1.union(set2)).map(lambda entry: list(entry[1]))
    
    # print(basket_rdd.filter(lambda entry: ("1",) in entry).count())
    
    # return
    
    # 3. Start SON/A-Priori
    
    basket_count = basket_rdd.count()
    
    size = 1
    
    candidates = []
    frequent_itemsets = []

    while True:
        
        # print(f"Itemset size Outside function: {size}")
        
        candidate_set = basket_rdd.mapPartitions(lambda iterator: aPriori(iterator, threshold, basket_count)).reduce(lambda set1, set2: set1.union(set2))
        
        if len(candidate_set) == 0:
            break
        
        candidate_list = list(candidate_set)
        for i in range(len(candidate_list)):
            candidate_list[i] = list(candidate_list[i])
            
        candidates.append(candidate_list)
        
        valid_candidate_list = validateCandidates(basket_rdd, candidate_set, threshold).map(lambda entry: list(entry)).collect()
        
        if len(valid_candidate_list) > 0:
            frequent_itemsets.append(valid_candidate_list)
        
        basket_rdd = basket_rdd.mapPartitions(lambda iterator: getCandidatesForNextSize(iterator, candidate_set, size)).cache()
        
        size += 1
    
    # 5. Output the answer
    
    out["Candidates"] = candidates
    
    out["Frequent Itemsets"] = frequent_itemsets
    
    out["Num Candidates By Size"] = {}
    
    out["Num Candidates Total"] = 0
    
    for i in range(len(candidates)):
        out["Num Candidates By Size"][str(i+1)] = len(candidates[i])
        out["Num Candidates Total"] += len(candidates[i])
    
    time1 = time.time()
    out['Runtime'] = time1-time0
    with open(outputJson, 'w') as f:
        json.dump(out, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HW2T1')	
    parser.add_argument('--y', type=int, default=2016, help ='Filter year')
    parser.add_argument('--c', type=int, default=1, help ='case number')
    parser.add_argument('--r', type=int, default=0, help ='filter review rate')
    parser.add_argument('--t', type=int, default=10, help ='frequent threshold')
    parser.add_argument('--input_file', type=str, default='../data/small2.csv', help ='input file')
    parser.add_argument('--output_file', type=str, default='./HW2task1.json', help ='output  file')
    
    args = parser.parse_args()
    case = args.c
    threshold = args.t
    inputJson = args.input_file
    outputJson = args.output_file
    year_filter = args.y
    time0 = time.time()
    
	# Read Input
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    rdd = sc.textFile(inputJson)
    main(rdd, case, threshold, outputJson, year_filter)
    sc.stop()