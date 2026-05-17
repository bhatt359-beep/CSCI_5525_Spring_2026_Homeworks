import argparse
import json
import time
import pyspark
import random
import math
import shutil
import os

# from tqdm import tqdm

def gcd(x: int, y: int):
    if y > x:
        return gcd(y, x)
    if y == 0:
        return x
    return gcd(y, x%y)

def business_hash(business_index: int, i: int, M: int, P: int, a1: list[int], b1: list[int], a2: list[int], b2: list[int]):

    h1 = (a1[i]*(business_index+1) + b1[i]) % P
    h2 = (a2[i]*(business_index+1) + b2[i]) % P
    hi = (h1 + h2*(i+1)) % M  
      
    return hi 


def signature(entry: tuple, business_dict: dict, M: int, P: int, a1: list[int], b1: list[int], a2: list[int], b2: list[int]):    
    business_indexes = []
    
    for business_id in entry[1]:
        business_indexes.append(business_dict[business_id])
    
    signature = []
    
    for i in range(100):
        min_hash = M
        
        for business_index in business_indexes:
            hash_val = business_hash(business_index, i, M, P, a1, b1, a2, b2)
                
            min_hash = min(min_hash, hash_val)
            
        signature.append(min_hash)
    
    return (entry[0], signature)

def band(entry: tuple):    
    r = 2
    b = 100 // r
    bands = []
    
    for i in range(b):            
        bands.append(((tuple(entry[1][r*i:r*(i+1)]), i), entry[0]))
    
    return bands    

def get_candidate_pairs(entry: tuple):
    candidates = sorted(list(entry[1]))
    
    candidate_pair_list = []
    
    for i in range(len(candidates)):
        for j in range(i+1, len(candidates)):
            candidate_pair = {
                "u1": candidates[i],
                "u2": candidates[j]
            }
            candidate_pair_list.append(candidate_pair)
    
    return candidate_pair_list

def jaccard_sim(entry: tuple, user_business_dict: dict):
    intersection_len = len(user_business_dict[entry["u1"]].intersection(user_business_dict[entry["u2"]]))
    union_len = len(user_business_dict[entry["u1"]].union(user_business_dict[entry["u2"]]))
    
    return entry | {"sim": intersection_len/union_len}

def find_prime(lower_bound: int):
    factor_dict = {}

    for i in range(2, lower_bound**2):
        if i not in factor_dict:
            # Number is Prime
            # print(i)
            factor_dict[i**2] = [i]
            if i >= lower_bound:
                return i
        else:
            # Number is not Prime
            curr_prime_list = factor_dict[i]
            del factor_dict[i]
            for prime in curr_prime_list:
                if i + prime not in factor_dict:
                    factor_dict[i + prime] = []
                factor_dict[i + prime].append(prime)

def minHashJacDiff(entry: dict, signature_dict: dict):
    sig1 = signature_dict[entry["u1"]]
    sig2 = signature_dict[entry["u2"]]
    
    match_count = 0
    
    for i in range(100):
        if sig1[i] == sig2[i]:
            match_count += 1
    
    min_hash_sim = match_count/100
    
    diff = min_hash_sim - entry["sim"]
    
    return diff
    

def main(input_file, candidate_file, output_file, jac_thr, seed, sc,
         ground_truth_file
         ):
    """
    Write your own code here
    """
    random.seed(seed)
    # print(f"Input File: {input_file}")
    
    input_rdd = sc.textFile(input_file).map(lambda entry: json.loads(entry)).cache()
    
    user_business_rdd = input_rdd.map(lambda entry: (entry["user_id"], entry["business_id"]))
    user_business_rdd = user_business_rdd.groupByKey()
    user_business_rdd = user_business_rdd.map(lambda entry: (entry[0], set(entry[1]))).cache()
    
    user_business_dict = user_business_rdd.collectAsMap()
    
    business_dict = input_rdd.map(lambda entry: entry["business_id"]).distinct().zipWithIndex().collectAsMap()
    
    M = len(business_dict)
    P = 2147483647
    
    # print(f"M: {M}")
    # print(f"P: {P}")

    a1 = []
    b1 = []
    a2 = []
    b2 = []
    
    for i in range(100):
        a1i = random.randint(1, P-1)
        b1i = random.randint(1, P-1)
        a2i = random.randint(1, P-1)
        b2i = random.randint(1, P-1)
        
        a1.append(a1i)
        b1.append(b1i)
        a2.append(a2i)
        b2.append(b2i)
    
    partition_count = user_business_rdd.getNumPartitions()
    
    signature_rdd = user_business_rdd.map(lambda entry: signature(entry, business_dict, M, P, a1, b1, a2, b2)).cache()
    
    band_rdd = signature_rdd.repartition(partition_count*10).flatMap(band)
    
    band_rdd = band_rdd.groupByKey()
 
    candidate_pair_rdd = band_rdd.flatMap(get_candidate_pairs).map(json.dumps).distinct().map(json.loads)
    
    jaccard_rdd = candidate_pair_rdd.map(lambda entry: jaccard_sim(entry, user_business_dict)).cache()
    
    unit_test = False
    
    if unit_test:
        jaccard_rdd.count()
        print("Starting Unit Test")
        # signature_dict = signature_rdd.collectAsMap()
        
        # fp_diff_rdd = jaccard_rdd.filter(lambda entry: entry["sim"] < jac_thr).map(lambda entry: minHashJacDiff(entry, signature_dict))
        
        # false_positives = fp_diff_rdd.count()
        
        # print(fp_diff_rdd.sortBy(lambda entry: -abs(entry)).take(false_positives // 8)[-1])
        
        ground_truth_rdd = sc.textFile(ground_truth_file).map(lambda entry: json.loads(entry))
        
        true_positives = jaccard_rdd.filter(lambda entry: entry["sim"] >= jac_thr).count()
        
        precision = true_positives/jaccard_rdd.count()
        recall = true_positives/ground_truth_rdd.count()
        
        print(f"Precision: {precision}")
        print(f"Recall: {recall}")
        print(f"F1: {2/((1/precision) + (1/recall))}")
        
        # print(f"Min Similarity: {jaccard_rdd.map(lambda entry: entry['sim']).min()}")
    else:
        # print("Creating output files")
        candidate_pair_list = candidate_pair_rdd.map(lambda entry: json.dumps(entry) + '\n').collect()
        with open(candidate_file, "w+") as f:
            f.writelines(candidate_pair_list)
            
        jaccard_list = jaccard_rdd.filter(lambda entry: entry["sim"] >= jac_thr).map(lambda entry: json.dumps(entry) + '\n').collect()
        with open(output_file, "w+") as f:
            f.writelines(jaccard_list)

if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw3') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A3')
    parser.add_argument('--input_file', type=str, default='./data/tr1.json')
    parser.add_argument('--candidate_file', type=str, default='./outputs/candidate.out')
    parser.add_argument('--output_file', type=str, default='./outputs/task.out')
    parser.add_argument('--time_file', type=str, default='./outputs/task.time')
    parser.add_argument('--threshold', type=float, default=0.3)
    parser.add_argument('--seed', type=float, default=0)
    parser.add_argument('--ground_truth_file', type=str, default='./data/tr1-gt.out')
    args = parser.parse_args()

    main(args.input_file, args.candidate_file, args.output_file, 
         args.threshold, args.seed , sc,
         args.ground_truth_file
         )
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))

    




    