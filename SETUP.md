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
