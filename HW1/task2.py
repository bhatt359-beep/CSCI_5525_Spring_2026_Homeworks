import argparse
import pyspark
import json

def main(args):
    sc_conf = pyspark.SparkConf().setAppName('task2').setMaster('local[*]').set('spark.driver.memory','8g').set('spark.executor.memory','4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    '''
    YOUR CODE HERE
    '''
    
    review_rdd = sc.textFile(args.review_file).map(lambda entry: json.loads(entry))
    review_rdd = review_rdd.map(lambda entry: (entry['business_id'], entry['review_id']))
    
    business_rdd = sc.textFile(args.business_file).map(lambda entry: json.loads(entry)).cache()
    business_rdd = business_rdd.map(lambda entry: (entry['business_id'], entry['state']))
    
    state_review_rdd = business_rdd.join(review_rdd).map(lambda entry: entry[1])
    
    result = state_review_rdd.groupBy(lambda entry: entry[0]).map(lambda entry: (entry[0], len(entry[1]))).sortBy(lambda entry: -entry[1]).map(lambda entry: list(entry)).take(args.n)

    output = {"result": result}
    
    with open(args.output_file, 'w') as f:
        json.dump(output, f)
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='A1T2')
    parser.add_argument('--review_file', type=str, default='./data/review.json', help='review file')
    parser.add_argument('--business_file', type=str, default='./data/business.json', help='business file')
    parser.add_argument('--output_file', type=str, default='./a1t2.json', help='the output file contains your answers')
    parser.add_argument('--n', type=int, default=5, help='top n states by number of reviews')
    args = parser.parse_args()
    
    main(args)

