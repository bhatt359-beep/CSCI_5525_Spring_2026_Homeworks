
import pyspark
import argparse
import json
import math
import time

def jaccard_sim(user_entry, business_entry, model_dict):
    if user_entry not in model_dict:
        return 0
        
    if business_entry not in model_dict:
        return 0
    
    user_entry_set = model_dict[user_entry]
    business_entry_set = model_dict[business_entry]
    
    union_len = len(user_entry_set.union(business_entry_set))
    intersection_len = len(user_entry_set.intersection(business_entry_set))
    
    return intersection_len/union_len

def main(test_file, model_file, output_file, sc):
    """
    Your code is here
    """
    
    model_rdd = sc.textFile(model_file).map(lambda entry: list(json.loads(entry).values())[0])
    
    model_dict = model_rdd.map(lambda entry: (entry[0], set(entry[1]))).collectAsMap()

    test_rdd = sc.textFile(test_file).map(json.loads).map(lambda entry: entry | {"sim": jaccard_sim(entry["user_id"], entry["business_id"], model_dict)})
    
    with open(output_file, 'w+') as f:
        f.writelines(test_rdd.filter(lambda entry: entry["sim"] >= .01).map(lambda entry: json.dumps(entry) + '\n').collect())
    

if __name__ == '__main__':
    start_time = time.time()

    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4_task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='hw4-task1-predict')
    parser.add_argument('--test_file',   type=str, default='./data/val_review.json')
    parser.add_argument('--model_file',  type=str, default='./data/task1.model')
    parser.add_argument('--output_file', type=str, default='./data/task1.val.out')
    parser.add_argument('--time_file',   type=str, default='./data/task1_predict.time')
    args = parser.parse_args()

    main(args.test_file, args.model_file, args.output_file, sc)
    sc.stop()

    with open(args.time_file, 'w') as f:
        json.dump({'time': time.time() - start_time}, f)
    print('Duration:', time.time() - start_time)


