import argparse
import pyspark
import json
from operator import add
from datetime import datetime
   
def transform_words(words: list[str], stopword_set: set[str]):
    result = []
    
    for word in words:
        transformed_word = word.lower().strip("?!.;:[](),")
        if transformed_word not in stopword_set and len(transformed_word) > 0:
            result.append(transformed_word)
    
    return result

def filtered_word_rdd(rdd: pyspark.rdd.RDD, stopword_set, key1: str = "date", key2: str = "text", year = 2015):
    entries = rdd.filter(lambda entry: datetime.fromisoformat(entry[key1]).year >= year)
    entries = entries.map(lambda entry: (datetime.fromisoformat(entry[key1]).month, entry[key2].split()))
    entries = entries.map(lambda entry: (entry[0], transform_words(entry[1], stopword_set)))
    
    return entries

def month_transform(month_int: int):
    month_str = str(month_int)
    if len(month_str) < 2:
        month_str = "0" + month_str
    return month_str

def map_func(entry):
    result = []
    
    for word in entry[1]:
        result.append((word, 1))
    
    return result


def main(args):
    sc_conf = pyspark.SparkConf().setAppName('task1').setMaster('local[*]').set('spark.driver.memory','8g').set('spark.executor.memory','4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    '''
    YOUR CODE HERE
    '''
    
    output = {}
    
    rdd = sc.textFile(args.input_file).map(lambda entry: json.loads(entry)).cache()
    
    stopword_set = set(open(args.stopwords, 'r').read().split())
    
    # print(rdd.first().keys())
    
    # Part A
    
    count = rdd.count()
    sum = rdd.map(lambda entry: entry['stars']).reduce(add)
    
    output["A"] = sum/count
    
    # Part B
    
    output["B"] = rdd.filter(lambda entry: datetime.fromisoformat(entry["date"]).year != args.t_y).count()
    
    # Part C
    
    output["C"] = rdd.map(lambda entry: [month_transform(datetime.fromisoformat(entry["date"]).month), entry["review_id"]]).groupByKey().map(lambda entry: (entry[0], len(entry[1]))).sortBy(lambda entry: -entry[1]).take(args.n)
    
    # Filter Words
    
    rdd = filtered_word_rdd(rdd, stopword_set, year=args.t_y).cache()
    
    # print(rdd.flatMap(lambda entry: entry[1]).filter(lambda entry: len(entry) > args.m_l).distinct().collect())
    
    # Part D
    
    output["D"] = rdd.map(lambda entry: (month_transform(entry[0]), len(entry[1]))).reduceByKey(add).map(lambda entry: list(entry)).collect()
    
    # assert_problem_d(output["D"], stopword_set)
    
    # return
    
    # Part E
    
    sum = rdd.map(lambda entry: len(entry[1])).reduce(add)
    count = rdd.count()
    
    output["E"] = sum/count
    
    # Part F
    
    output['F'] = rdd.flatMap(lambda entry: map_func(entry)).filter(lambda entry: len(entry[0]) > args.m_l).reduceByKey(add).sortBy(lambda entry: -entry[1]).map(lambda entry: entry[0]).take(args.i)
    
    # print(output)
    
    with open(args.output_file, 'w') as file:
        json.dump(output, file, indent=4)
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HW1T1')
    parser.add_argument('--input_file', type=str, default='./data/review.json', help='input file')
    parser.add_argument('--output_file', type=str, default='./a1t1.json', help='output file')
    parser.add_argument('--stopwords', type=str, default='./data/stopwords', help='stopword file')
    parser.add_argument('--t_y', type=int, default=2015, help='year')
    parser.add_argument('--m_l', type=int, default=3, help='minimum word length')
    parser.add_argument('--n', type=int, default=5, help='top n months')
    parser.add_argument('--i', type=int, default=10, help='top i frequent words')

    args = parser.parse_args()
    main(args)