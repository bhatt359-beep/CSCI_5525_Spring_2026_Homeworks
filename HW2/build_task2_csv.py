import argparse
import pyspark
import json
import pandas as pd
from tqdm import tqdm

def main(args):
    sc_conf = pyspark.SparkConf().setAppName('task2').setMaster('local[*]').set('spark.driver.memory','8g').set('spark.executor.memory','4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    '''
    YOUR CODE HERE
    '''
    
    az_business_ids = set(sc.textFile(args.business_file).map(lambda entry: json.loads(entry)).filter(lambda entry: entry['state'] == 'AZ').map(lambda entry: entry['business_id']).collect())
    
    user_business = sc.textFile(args.review_file).map(lambda entry: json.loads(entry)).filter(lambda entry: entry['business_id'] in az_business_ids).map(lambda entry: [entry['user_id'], entry['business_id']]).collect()
    
    print(user_business[:5])
    
    user_business_df = pd.DataFrame(user_business)
    
    print(user_business_df.head())
    
    user_business_df.to_csv(args.output_file)
    
    sc.stop()
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='A1T2')
    parser.add_argument('--review_file', type=str, default='./data/review.json', help='review file')
    parser.add_argument('--business_file', type=str, default='./data/business.json', help='business file')
    parser.add_argument('--output_file', type=str, default='./data/user_business.csv', help='the output file contains your answers')
    parser.add_argument('--n', type=int, default=5, help='top n states by number of reviews')
    args = parser.parse_args()
    
    main(args)