import pyspark
import numpy as np
import random as rand
from tqdm import tqdm
import argparse
from operator import add

def transform_entry(mat_entry: tuple, ismat1: bool, changed_dim_len: int):
    result = []
    
    nums = mat_entry[0]
    
    for i in range(len(nums)):
        base_coords = [mat_entry[1], i]
        
        if ismat1:
            changed_dim = 2
            base_coords.append(0)
        else:
            changed_dim = 0
            base_coords.insert(0, -1)
            
        for j in range(changed_dim_len):
            base_coords[changed_dim] = j
            result.append((tuple(base_coords), nums[i]))
    
    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument("--low", type=int, default=1)
    parser.add_argument("--high", type=int, default=100)
    parser.add_argument("--min_dim", type=int, default=1)
    parser.add_argument("--max_dim", type=int, default=100)
    
    args = parser.parse_args()
    
    sc_conf = pyspark.SparkConf().setAppName('task2').setMaster('local[*]').set('spark.driver.memory','8g').set('spark.executor.memory','4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")
    
    # Generate indexes
    rng = np.random.default_rng()
    
    trials = args.trials
    
    low = args.low
    high = args.high
        
    for trial in tqdm(range(trials)):
        
        dim1 = rand.randint(args.min_dim, args.max_dim)
        dim2 = rand.randint(args.min_dim, args.max_dim)
        dim3 = rand.randint(args.min_dim, args.max_dim)
        
        mat1 = rng.integers(low=low, high=high, size=(dim1, dim2))
        mat2 = rng.integers(low=low, high=high, size=(dim2, dim3))
        
        # PySpark Implementation
        
        ## Phase 1
        
        mat1_rdd = sc.parallelize(mat1.tolist()).zipWithIndex()
        mat2_rdd = sc.parallelize(mat2.tolist()).zipWithIndex()
        
        mat1_rdd = mat1_rdd.flatMap(lambda entry: transform_entry(entry, True, dim3))
        mat2_rdd = mat2_rdd.flatMap(lambda entry: transform_entry(entry, False, dim1))
        
        prod_rdd = mat1_rdd.join(mat2_rdd)
        
        ## Phase 2
        prod_rdd = prod_rdd.map(lambda entry: ((entry[0][0], entry[0][2]), entry[1][0]*entry[1][1]))
        prod_rdd = prod_rdd.reduceByKey(add)
        
        # Compare to numpy
        
        np_prod = mat1 @ mat2
        
        assert prod_rdd.filter(lambda entry: np_prod[entry[0][0], entry[0][1]] != entry[1]).count() == 0, "Pyspark Product is Wrong"
    
    
if __name__ == "__main__":
    main()
