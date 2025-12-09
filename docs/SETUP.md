# Setup

## Setup lfs

This project uses [git lfs](https://git-lfs.com).

```shell
# mac os 
brew install git-lfs
git lfs install

# pull csv files from lfs
git lfs pull
```

## Setup Hadoop

```shell
docker compose up -d
```

## Load data to HDFS

```shell
docker exec -it resourcemanager bash

# create input directory in hdfs and copy data to it
hdfs dfs -mkdir -p /input
hdfs dfs -put -f /opt/hadoop/jobs/*.csv /input/

# check
hdfs dfs -ls /input
```

## Run job

```shell
docker exec -it resourcemanager bash

# example hadoop jar /opt/hadoop/jobs/sales-analytics.jar com.quicklybly.itmo.salesanalytics.linecount.LineCountDriver /input /output_count2
hadoop jar /opt/hadoop/jobs/sales-analytics.jar <driver-class> /input <output_folder>
```

## Check results

```shell
docker exec -it resourcemanager bash

hdfs dfs -cat /<output_folder>/part-r-00000
```

## Data Generation

For large data generation use `data_generator.py` script, change `TARGET_SIZE_MB` if needed.

```shell
python3 data_generator.py
```

As a result you will see `sales_heavy.csv` file in `jobs` directory, load it to hdfs using steps above.
