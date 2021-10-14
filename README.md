# Pyspark-cheatsheet

Cheatsheet for Pysppark. 

Tested this on AWS EMR.
Created a cluster - 1 master and 2 workers with following (Advanced Options when creating cluster): Hive 2.3.7, Pig 0.17.0, Hue 4.9.0, JupyterEnterpriseGateway 2.1.0, Spark 2.4.7

EMR Pricing: https://aws.amazon.com/emr/pricing/

TIPs,
1) It is better to define schema prior to import rather than infer schema. Infer schema causes job to read file multiple times
2) More workers better than larger workers
3) More files better than large ones. Preferable for files to be around same size
4) .parquet files are faster and work around some issues surrounding .csv
5) Sparks csv parser automatically ignores blank lines, can ignore comments if specified (e.g. df1 = spark.read.csv('datafile.csv.gz', comment='#'))
6) Spark Dataframes are immutable.
7) Spark uses Lazy processing - transformations are not run till an action is called and might be reordered. Keep that in mind
8) Reshuffling of data - need to avoid that as it slows processing. JOIN operations can reshuffle data
9) Can cache using .cache()
10) MLlib requires RDD and requires features to be numerical (e.g One Hot Encode categorical vars) This is dependent on model.e.g. for Random Forest a simple string indexer is fine
11) Also models handle missing data and scaling differently. Random forst does not need Scaling. Linear Regression and KNN do. Missing values can be replaced by something out of range for Random Forest. KNN and Regression need more thought
