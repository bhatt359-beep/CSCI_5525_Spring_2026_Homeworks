import pyspark
import json
import argparse
import datetime

def f(iterator):
    yield len(list(iterator))

def main(args):
    sc_conf = pyspark.SparkConf().setAppName('task3_d').setMaster('local[*]').set('spark.driver.memory','8g').set('spark.executor.memory','4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    '''
    YOUR CODE HERE
    '''

    rdd = sc.textFile(args.input_file).map(lambda entry: json.loads(entry))
    rdd = rdd.map(lambda entry: (datetime.datetime.fromisoformat(entry['date']).year, entry['review_id']))
    rdd = rdd.groupByKey().map(lambda entry: (entry[0], len(entry[1])))
    rdd = rdd.filter(lambda entry: entry[1] > args.n).map(lambda entry: list(entry))

    output = {
        "n_partitions": rdd.getNumPartitions(),
        "n_items": rdd.mapPartitions(f).collect(),
        "result": rdd.collect()
    }

    with open(args.output_file, 'w') as file:
        json.dump(output, file, indent=4)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A1T3_default')
    parser.add_argument('--input_file', type=str, default='./data/review.json', help='review file')
    parser.add_argument('--output_file', type=str, default='./a1t3_default.json', help='the output file contains your answers')
    parser.add_argument('--n', type=int, default=10000, help='review count threshold per year')
    args = parser.parse_args()
    main(args)

