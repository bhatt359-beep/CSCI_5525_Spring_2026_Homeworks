import argparse
import json
import time
import pyspark
import math
import itertools
import statistics
import random
from operator import add

def business_pairs(entry):
    business_ids = list(entry[1])
    result = []
    
    for i in range(len(business_ids)):
        for j in range(i, len(business_ids)):
            if business_ids[j] < business_ids[i]:
                result.append((business_ids[j], business_ids[i]))
            else:
                result.append((business_ids[i], business_ids[j]))
    
    return result

def pearson_score(entry, user_set_map, review_map, avg_business_rating_map, use_all_ratings=False):
    co_rated_users = user_set_map[entry[0]].intersection(user_set_map[entry[1]])
    
    if use_all_ratings:
        avg_0 = avg_business_rating_map[entry[0]]
        avg_1 = avg_business_rating_map[entry[1]]
    else:
        sum_0 = 0
        sum_1 = 0
        
        for user_id in co_rated_users:
            sum_0 += review_map[(user_id, entry[0])]
            sum_1 += review_map[(user_id, entry[1])]
        
        avg_0 = sum_0/len(co_rated_users)
        avg_1 = sum_1/len(co_rated_users)
    
    cov = 0
    sq_diff_sum_0 = 0
    sq_diff_sum_1 = 0
    
    for user_id in co_rated_users:
        term_0 = review_map[(user_id, entry[0])] - avg_0
        term_1 = review_map[(user_id, entry[1])] - avg_1
        
        cov += term_0*term_1
        sq_diff_sum_0 += term_0*term_0
        sq_diff_sum_1 += term_1*term_1
    
    if sq_diff_sum_0*sq_diff_sum_1 == 0:
        sim = 0
    else:
        sim = cov/(math.sqrt(sq_diff_sum_0*sq_diff_sum_1))
    
    return {
        'b1': entry[0],
        'b2': entry[1],
        'sim': sim,
        'num_co_rated': len(co_rated_users)
    }
    
        
def main(train_file, model_file, co_rated_thr, sc):
    """
    Your code is here
    """    
    train_rdd = sc.textFile(train_file).map(json.loads)
    
    train_rdd = train_rdd.map(lambda review: ((review['user_id'], review['business_id']), review['stars'])).groupByKey()
    
    # print(train_rdd.filter(lambda entry: entry[0][1] == "S-pwdOmtIPL2UCjQEvKROg").mapValues(len).filter(lambda entry: entry[1] > 1).collect())
    
    # return
    
    train_rdd = train_rdd.mapValues(statistics.median).cache()
    
    # train_rdd = train_rdd.mapValues(lambda entry: list(entry)[0]).cache()
    
    # print(train_rdd.take(5))
    
    # return
    
    review_map = train_rdd.collectAsMap()
    
    train_rdd = train_rdd.map(lambda entry:{
        'user_id': entry[0][0],
        'business_id': entry[0][1],
        'stars': entry[1]
    }).cache()
    
    # # Debug block
    
    # curr_business_id = 'HK6Iew4GgRzV6A5zO2OXjw'
    
    # curr_users = ['RpuHjlLqJBIQoszxwBokzw', '5VFQdMIC5tpp2bxine0sXw', '94nuNE5-Eh8XMT7QVC_uJQ', 'ylE_w4QR7JCz9cr9ub9l3A']
    
    # print(train_rdd.filter(lambda entry: entry['business_id'] == curr_business_id and 
    #                        entry['user_id'] in curr_users).collect())
    
    # print(train_rdd.filter(lambda entry: entry['business_id'] == curr_business_id and 
    #                        entry['user_id'] not in curr_users).count())
    
    # print(train_rdd.filter(lambda entry: entry['business_id'] == curr_business_id).map(lambda entry: entry['stars']).min())
    
    # return
    
    # Calculate IUF
    
    user_count = train_rdd.map(lambda entry: entry['user_id']).distinct().count()
    
    user_set_rdd = train_rdd.map(lambda entry: (entry['business_id'], set([entry['user_id']])))
    user_set_rdd = user_set_rdd.reduceByKey(lambda set1, set2: set1.union(set2)).cache()
    
    user_set_map = user_set_rdd.collectAsMap()
    
    iuf_rdd = user_set_rdd.mapValues(lambda entry: math.log2(user_count/len(entry)))
    iuf_dict = iuf_rdd.collectAsMap()
    
    train_rdd = train_rdd.map(lambda entry:{
        'user_id': entry['user_id'],
        'business_id': entry['business_id'],
        'stars': entry['stars']*iuf_dict[entry['business_id']]
    }).cache()
    
    # Calculate pearson correlation
    
    avg_business_rating_rdd = train_rdd.map(lambda entry: (entry['business_id'], (entry['stars'], 1)))
    avg_business_rating_rdd = avg_business_rating_rdd.reduceByKey(lambda entry1, entry2: (entry1[0]+entry2[0], entry1[1]+entry2[1]))
    avg_business_rating_rdd = avg_business_rating_rdd.mapValues(lambda entry: entry[0]/entry[1])
    avg_business_rating_map = avg_business_rating_rdd.collectAsMap()
    
    business_pair_rdd = train_rdd.map(lambda entry: (entry['user_id'], entry['business_id']))
    business_pair_rdd = business_pair_rdd.groupByKey()
    business_pair_rdd = business_pair_rdd.flatMap(business_pairs)
    
    business_pair_rdd = business_pair_rdd.filter(lambda entry: len(user_set_map[entry[0]].intersection(user_set_map[entry[1]])) >= co_rated_thr).distinct()
    
    pearson_score_rdd = business_pair_rdd.map(lambda entry: pearson_score(entry, user_set_map, review_map, avg_business_rating_map, use_all_ratings=False)).cache()
    
    # test_pair_set = set([("Y3xeBrwZ0BABw-AQr3TR0g", "QHWYlmVbLC3K6eglWoHVvA"), ("M_EpyAH1CZZVlhxfYBLOqg", "7pwZZVVlYCxQvVdd8Q03wg"),
    #                   ("bOAGspD-hGiF7RcdT3dHdg", "bgxDswHIdFP0Go0pNfyAAw"), ("LVNt93fRgTR0C-eubUshbQ", "QuU-a6FEcC_aZTgZkYFeLA"),
    #                   ("9PZxjhTIU7OgPIzuGi89Ew", "GVWcwqUD-ekCMQ0x2Ke6Ww"), ("GmomRGW_omclyKXKDppsIg", "MzFhaFNbE03zF84BPkN7yQ"),
    #                   ("Al2YIlvkieMwo5hW_ZBgBA", "RqW9S4WG9UYZHKhHRHXJZg"), ("Co3Ogqy6y2JgZdG0wBlrUQ", "gjFUrbSMo1rA1jgG5IhA2A"),
    #                   ("pc8D9b_rnL_I8ak91tKjsg", "S-pwdOmtIPL2UCjQEvKROg"), ("ev8KX9xeLe9fP9y-vV81tQ", "ev8KX9xeLe9fP9y-vV81tQ")])
    
    # test_pairs = pearson_score_rdd.filter(lambda entry: (entry['b1'], entry['b2']) in test_pair_set or (entry['b2'], entry['b1']) in test_pair_set).collect()
    
    # for test_pair in test_pairs:
    #     print(test_pair)
    
    # print(pearson_score_rdd.count())
    
    with open(model_file, 'w+') as f:
        f.writelines(pearson_score_rdd.filter(lambda entry: entry['sim'] >= .2).map(lambda entry: json.dumps(entry) + '\n').collect())
    

if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4_build') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='hw4-task2-build')
    parser.add_argument('--train_file', type=str, default='./data/train_review.json')
    parser.add_argument('--model_file', type=str, default='./data/task2.model')
    parser.add_argument('--time_file',  type=str, default='./data/task2_build.time')
    parser.add_argument('--m',          type=int, default=3)
    args = parser.parse_args()

    main(args.train_file, args.model_file, args.m, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))