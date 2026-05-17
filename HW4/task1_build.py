import pyspark
import argparse
import json
import math
import time
import string
import re
import heapq
from operator import add
from collections import Counter

def preprocess(entry: dict):        
    word_list = entry['text'].split()
    
    result = []
    
    for word in word_list:
        transformed_word = ""
        
        list_word_lower = list(word.lower())
        
        for letter in list_word_lower:
            if letter not in string.punctuation and not letter.isdigit():
                transformed_word = transformed_word + letter
        
        if transformed_word and len(transformed_word) > 0:
            result.append({
                "word": transformed_word, 
                "user_id": entry["user_id"], 
                "business_id": entry["business_id"]
            })
            
    return result

def eliminate_rare_words(entry, stopword_set):
    words = list(entry[1])
    
    counter = {}
    
    for word in words:
        if word not in counter:
            counter[word] = 0
        
        counter[word] += 1
    
    result = []
    
    for word in words:
        if counter[word]/len(words) > .0001 and word not in stopword_set:
            result.append((word, entry[0]))
    
    return result

def extract_words(entry):
    
    result = set()
    
    for tup in entry:
        result.add(tup[1])
    
    return result

def word_frequencies(entry):
    words = list(entry[1])
    
    counter = {}
    
    for word in words:
        if word not in counter:
            counter[word] = 0
        
        counter[word] += 1
    
    result = []
    
    for word in counter:
        result.append({
            'word': word,
            'business_id': entry[0],
            'freq': counter[word]/len(words)
        })
    
    return result

def main(train_file, model_file, stopwords_file, sc):
    """
    Your code is here
    """
    # Preprocessing Data
    
    train_rdd = sc.textFile(train_file).map(json.loads).cache()

    stopword_set = set(open(stopwords_file, 'r').read().split())
    
    train_rdd = train_rdd.flatMap(lambda entry: preprocess(entry)).cache()
    
    word_count_rdd = train_rdd.filter(lambda entry: entry['word'] not in stopword_set).map(lambda entry: (entry['word'], 1)).reduceByKey(add).cache()
    
    total_word_count = word_count_rdd.map(lambda entry: entry[1]).reduce(add)
    
    filter_word_set = set(word_count_rdd.filter(lambda entry: entry[1]/total_word_count > .0001 and entry[0] not in stopword_set).map(lambda entry: entry[0]).collect())
    
    business_doc_rdd = train_rdd.filter(lambda entry: entry['word'] in filter_word_set).map(lambda entry: (entry["word"], entry["business_id"]))
    
    # review_words = business_doc_rdd.filter(lambda entry: entry[1] == "YLKRxqxFwME0vywp3CAIpA").map(lambda entry: entry[0]).collect()
    
    # print(review_words)
    
    # counter = {}
    
    # max_count = 0
    
    # for word in review_words:
    #     if word not in counter:
    #         counter[word] = 0
        
    #     counter[word] += 1
    #     max_count = max(counter[word], max_count)
        
    # diner_tf = counter['diner']/max_count
    
    # print(f"Diner TF: {diner_tf}")
    
    # diner_count = business_doc_rdd.filter(lambda entry: entry[0] == 'diner').distinct().count()
    
    # business_count = business_doc_rdd.map(lambda entry: entry[1]).distinct().count()
    
    # diner_idf = math.log2(business_count/diner_count)
    
    # print(f"Diner IDF: {diner_idf}")
    
    # print(f"Diner TF.IDF: {diner_tf*diner_idf}")
    
    # return
    
    # Calculate TF
    
    count_per_business_rdd = business_doc_rdd.map(lambda entry: (entry, 1)).reduceByKey(add).cache()
    
    max_count_dict = count_per_business_rdd.map(lambda entry: (entry[0][1], entry[1])).groupByKey().mapValues(max).collectAsMap()
    
    tf_rdd = count_per_business_rdd.map(lambda entry: (entry[0], entry[1]/max_count_dict[entry[0][1]]))
    
    # print(tf_rdd.filter(lambda entry: entry[0][1] == "YLKRxqxFwME0vywp3CAIpA").collect())
    
    # Calculate IDF
    
    business_count = train_rdd.map(lambda entry: entry["business_id"]).distinct().count()
    
    idf_dict = train_rdd.map(lambda entry: (entry["word"], entry["business_id"])).distinct().map(lambda entry: (entry[0], 1)).reduceByKey(add).mapValues(lambda entry: math.log2(business_count/entry)).collectAsMap()
    
    tf_idf_rdd = tf_rdd.map(lambda entry: (entry[0], entry[1]*idf_dict[entry[0][0]]))
    
    
    # Find the top 100 words per business document

    business_profile_rdd = tf_idf_rdd.map(lambda entry: (entry[0][1], (entry[1], entry[0][0]))).groupByKey()
    
    business_profile_rdd = business_profile_rdd.mapValues(lambda entry: list(reversed(sorted(entry)))[:100])

    business_profile_rdd = business_profile_rdd.mapValues(lambda entry: extract_words(entry))
    
    # print(business_profile_rdd.filter(lambda entry: entry[0] == "blmmC5O_9fWK0cQ2hjGXWQ").first()[1])
    
    # return

    business_profile_dict = business_profile_rdd.collectAsMap()
    
    user_profile_rdd = train_rdd.map(lambda entry: (entry["user_id"], business_profile_dict[entry["business_id"]])).reduceByKey(lambda entry1, entry2: entry1.union(entry2))

    with open(model_file, 'w+') as f:
        f.writelines(business_profile_rdd.map(lambda entry: json.dumps({'business': [entry[0], list(entry[1])]}) + '\n').collect())
        f.writelines(user_profile_rdd.map(lambda entry: json.dumps({'user': [entry[0], list(entry[1])]}) + '\n').collect())

if __name__ == '__main__':
    start_time = time.time()

    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4_task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='hw4-task2-build')
    parser.add_argument('--train_file',     type=str, default='./data/train_review_text_150k.json')
    parser.add_argument('--model_file',     type=str, default='./data/task1.model')
    parser.add_argument('--stopwords_file', type=str, default='./data/stopwords')
    parser.add_argument('--time_file',      type=str, default='./data/task1_build.time')
    args = parser.parse_args()

    main(args.train_file, args.model_file, args.stopwords_file, sc)
    sc.stop()

    with open(args.time_file, 'w') as f:
        json.dump({'time': time.time() - start_time}, f)
    print('Duration:', time.time() - start_time)


