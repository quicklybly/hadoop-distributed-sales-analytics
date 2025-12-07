# Setup

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
