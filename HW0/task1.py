from pyspark.sql import SparkSession
import json
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--input_file", type=str, default="data/gpa.json")
parser.add_argument("--output_file", type=str, default="ans_hw0.json")
args = parser.parse_args()

spark = SparkSession.builder.appName('hw0').getOrCreate()

print("Created Spark Session")

rdd = spark.read.format('json').load(args.input_file).toJSON()

gpa_sum = 0

for entry in rdd.collect():
  json_entry = json.loads(entry) 
  gpa_sum += json_entry["gpa"]

avg_gpa = gpa_sum/len(rdd.collect())

ans_hw0_dict = {"avg_gpa": avg_gpa}

ans_hw0_str = json.dumps(ans_hw0_dict)

f = open(args.output_file, 'w')
f.write(ans_hw0_str)
f.close()

print("Task Completed")
