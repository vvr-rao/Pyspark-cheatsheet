# Pyspark-cheatsheet

Cheatsheet for Pysppark. 

Tested this on AWS EMR.
Created a cluster - 1 master and 2 workers with following (Advanced Options when creating cluster): Hive 2.3.7, Pig 0.17.0, Hue 4.9.0, JupyterEnterpriseGateway 2.1.0, Spark 2.4.7

EMR Pricing: https://aws.amazon.com/emr/pricing/

TIPs,
1) better to define schema rather than infer schema. Infer schema causes job to read file multiple times
2) More workers better than larger workers
3) more files bettwr than large ones. Preferable for file to be around same size
4) .parquet files are faster and work around some issues surrounding .csv
5) Spark Dataframes are immutable.
6) Spark uses Lazy processing - transformations are not run till an action is called and might be reordered. Keep that in mind
7) Reshuffling of data - need to avoid that as it slows processing. JOIN operations can reshuffle data
8) Can cache using .cache()
9) MLlib requires RDD and requires features to be numerical (On Hot Encode categorical vars)
