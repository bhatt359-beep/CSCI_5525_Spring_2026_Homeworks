
import sys
import time
import argparse
import json
from itertools import islice, combinations
import pyspark
from pyspark import SparkContext
from operator import add
# from tqdm import tqdm

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
    
def pcy(iterator, threshold, basket_count, hashSize):
    """
    Implement the PCY (Park-Chen-Yu) algorithm:
    1. Process baskets to calculate partition-specific threshold
    2. For singleton pass, hash item pairs into a hash table
    3. Use hash table to identify potentially frequent pairs
    4. Generate candidates with a-priori pruning and hash-based filtering
    5. Validate candidates against data in each iteration
    6. Continue until no new frequent itemsets are found
    7. Return all frequent itemsets discovered
    
    HINT: The Major bottleneck for this data is the processing of the size-2 candidates. It is recomended you use a hashtable instead of a dictionary to store their counts. 
    During step 2 you can also create size-2 combinations from each basket, and hash the pair by index = hash(pair)%hashTableSize, and increment the corresponding entry by 1  hashTable[index]+=1
    """
    list_iterator = list(iterator)

    reduced_threshold = threshold*len(list_iterator)/basket_count
    
    hash_table = [0] *  hashSize
    
    counter = {}
    
    frequent_itemsets = set()

    for basket in list_iterator:
        for i in range(len(basket)):
            if basket[i] not in counter:
                counter[basket[i]] = 0
            
            hash_idx = hash(basket[i]) % hashSize
                
            hash_table[hash_idx] += 1
            counter[basket[i]] += 1
            
            if hash_table[hash_idx] >= reduced_threshold and counter[basket[i]] >= reduced_threshold:
                frequent_itemsets.add(basket[i])
    
            
    yield frequent_itemsets
   
   

def transformCSVLine(csv_line: str, key_names: list[str]):
    if csv_line[1] == 0:
        return []

    ids = csv_line[0].split(',')
    
    transformation = {}
    
    for i in range(len(ids)):
        transformation[key_names[i]] = ids[i]
    
    return [transformation]

def unit_test(basket_rdd, valid_candidate_set, threshold):
    count_dict = basket_rdd.flatMap(lambda entry: entry).map(lambda entry: (entry, 1)).countByKey()
    for candidate in valid_candidate_set:
        candidate_count = count_dict[tuple(candidate)]
        assert candidate_count >= threshold, f"{candidate} shows up {candidate_count} times when it should show up at least {threshold} times"
    
    print("Unit Test Passed")

def unit_test_apriori(basket_rdd, valid_candidate_set, threshold):
    assert basket_rdd.flatMap(lambda entry: entry).filter(lambda entry: entry not in valid_candidate_set).map(lambda entry: (entry, 1)).reduceByKey(add).filter(lambda entry: entry[1] >= threshold).count() == 0
    print("All Frequent Itemsets Found")
    

# Main function to orchestrate the workflow
def main(rdd, filter_threshold, support_threshold, outputJson, hashSize):
    """
    Main function controlling the workflow and implementing the SON algo:
    1. Preprocess data (remove header, ect)
    2. Build Case 1 market-basket model (user -> businesses)
    3. Filter users who reviewed more than filter_threshold businesses
    4. Apply the SON algorithm with pcy
    5. Output results
    """
	# write answer into a dictionary
    out = {}
    '''
    
    YOUR CODE HERE
    
    '''
    # 1. Preprocess Data
    rdd_keys = rdd.first().split(',')
    
    transformed_rdd = rdd.zipWithIndex().flatMap(lambda entry: transformCSVLine(entry, rdd_keys))
    
    # print(transformed_rdd.count())
    
    first_entry = transformed_rdd.first()
    
    if "rate" in first_entry and "year" in first_entry:
        print("Filtering Rate and Year")
        transformed_rdd = transformed_rdd.filter(lambda entry: entry["year"] == "2016" and entry["rate"] != "0")
    
    transformed_rdd = transformed_rdd.map(lambda entry: (entry['user_id'], (entry['business_id'],)))
    
    # 2. Convert data into baskets
    
    basket_rdd = transformed_rdd.map(lambda entry: (entry[0], set([entry[1]]))).reduceByKey(lambda set1, set2: set1.union(set2)).map(lambda entry: list(entry[1]))
    
    # print(basket_rdd.map(lambda entry: len(entry)).max())
    
    # return
    
    basket_rdd = basket_rdd.filter(lambda entry: len(entry) > filter_threshold)
    
    # print(basket_rdd.filter(lambda entry: ("1",) in entry).count())
    
    # return
    
    # 3. Start SON/PCY
    
    basket_count = basket_rdd.count()
    
    size = 1
    
    candidates = []
    frequent_itemsets = []

    while True:
        
        # print(f"Itemset size Outside function: {size}")
        
        candidate_set = basket_rdd.mapPartitions(lambda iterator: pcy(iterator, support_threshold, basket_count, hashSize)).reduce(lambda set1, set2: set1.union(set2))
        
        if len(candidate_set) == 0:
            break
        
        # print(f"Number of Candidates for size {size}: {len(candidate_set)}")
        
        candidate_list = list(candidate_set)
        for i in range(len(candidate_list)):
            candidate_list[i] = list(candidate_list[i])
            
        candidates.append(candidate_list)
        
        valid_candidates = validateCandidates(basket_rdd, candidate_set, support_threshold)
        
        valid_candidate_list = valid_candidates.map(lambda entry: list(entry)).collect()
        
        
        if len(valid_candidate_list) > 0:
            frequent_itemsets.append(sorted(valid_candidate_list))

        
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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Task 2: SON algorithm on Yelp data')
    parser.add_argument('--b', type=int, help='Filter threshold for users')
    parser.add_argument('--t', type=int, help='Support threshold for frequent itemsets')
    parser.add_argument('--input_file', type=str, help='Input file path')
    parser.add_argument('--output_file', type=str, help='Output file path')
    
    args = parser.parse_args()
    
    # Extract arguments
    filter_threshold = args.b
    support_threshold = args.t
    input_file = args.input_file
    output_file = args.output_file
    
    hashSize = 30000000
    
    # Record start time
    time0 = time.time()
    
    # Read Input
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    sc.setLogLevel("OFF")
    rdd = sc.textFile(input_file)
    main(rdd, filter_threshold, support_threshold, output_file, hashSize)
    sc.stop()