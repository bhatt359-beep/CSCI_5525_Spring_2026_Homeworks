
import argparse
import json
import time
import pyspark
import heapq
import statistics
from operator import add

def extract_useful_pairs(entry, user_business_set_map):
    result = []
    
    for business_id in user_business_set_map[entry['user_id']]:
        result.append((min(business_id, entry['business_id']), max(business_id, entry['business_id'])))
    
    return result

# def top_n(entry, n_weights):
#     pq = []
    
#     for tup in entry:
#         heapq.heappush(pq, tup)
#         if len(pq) > n_weights:
#             heapq.heappop(pq)
    
#     return pq

def mergeDicts(entry):
    entry_1 = entry[1]
    
    if entry_1 is None:
        entry_1 = {
            'sim': 0,
            'num_co_rated': 0
        }
    
    return entry[0] | entry_1

def predict(entry):
    result = {
        'user_id': entry[0][0],
        'business_id': entry[0][1]
    }
    
    numerator_sum = 0
    denominator_sum = 0
    
    for tup in entry[1]:        
        numerator_sum += tup[0]*tup[1]
        denominator_sum += abs(tup[0])
    
    if denominator_sum == 0:
        result['stars'] = 0
    else:
        result['stars'] = numerator_sum/denominator_sum
    
    result['stars'] = (result['stars'] + 5)/2
    
    return result


def main(train_file, test_file, model_file, output_file, n_weights, sc):
    """
    Your code is here
    """
    
    train_rdd = sc.textFile(train_file).map(json.loads).cache()
    model_rdd = sc.textFile(model_file).map(json.loads).cache()
    test_rdd = sc.textFile(test_file).map(json.loads).cache()
    
    # dupliate_predicion_rdd = test_rdd.map(lambda entry: ((entry['user_id'], entry['business_id']), 1)).reduceByKey(add).filter(lambda entry: entry[1] > 1)
    
    # print(dupliate_predicion_rdd.count())
    
    # return
    
    # target_pred_count = test_rdd.count()
    
    kv_test_rdd = test_rdd.map(lambda entry: (entry['user_id'], entry['business_id']))
    kv_train_rdd = train_rdd.map(lambda entry: (entry['user_id'], {
        'business_id': entry['business_id'],
        'stars': entry['stars']
    }))
    pair_rdd = kv_test_rdd.join(kv_train_rdd)
    
    # assert pair_rdd.map(lambda entry: (entry[0], entry[1][0])).distinct().count() == target_pred_count, f"Issue with join"
    
    pair_rdd = pair_rdd.map(lambda entry: ((min(entry[1][0], entry[1][1]['business_id']), max(entry[1][0], entry[1][1]['business_id'])), {
        'user_id': entry[0],
        'target_business_id': entry[1][0],
        'primary_business_id': entry[1][1]['business_id'],
        'stars': entry[1][1]['stars']
    }))
    weight_rdd = model_rdd.map(lambda entry: ((entry['b1'], entry['b2']), {
        'sim': entry['sim'],
        'num_co_rated': entry['num_co_rated']
    }))
    data_rdd = pair_rdd.leftOuterJoin(weight_rdd)
    data_rdd = data_rdd.mapValues(mergeDicts)
    
    top_n_rdd = data_rdd.map(lambda entry: ((entry[1]['user_id'], entry[1]['target_business_id']), (entry[1]['sim']*min(1, entry[1]['num_co_rated']/50), entry[1]['stars'])))
    top_n_rdd = top_n_rdd.groupByKey()
    top_n_rdd = top_n_rdd.mapValues(lambda entry: heapq.nlargest(n_weights, entry))
    
    output_rdd = top_n_rdd.map(predict)
    
    # output_pred_count = output_rdd.count()
    
    # assert output_pred_count == target_pred_count, f"Need {target_pred_count - output_pred_count} more predictions"
    
    with open(output_file, 'w+') as f:
        f.writelines(output_rdd.map(lambda entry: json.dumps(entry) + '\n').collect())

if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4_predict') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")


    parser = argparse.ArgumentParser(description='hw4-task2-predict')
    parser.add_argument('--train_file', type=str, default='./data/train_review.json')
    parser.add_argument('--test_file', type=str, default='./data/val_review.json')
    parser.add_argument('--model_file', type=str, default='./data/task2.model')
    parser.add_argument('--output_file', type=str, default='./data/task2.val.out')
    parser.add_argument('--time_file', type=str, default='./data/task2_predict.time')
    parser.add_argument('--n', type=int, default=3)
    args = parser.parse_args()

    main(args.train_file, args.test_file, args.model_file, args.output_file, args.n, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))