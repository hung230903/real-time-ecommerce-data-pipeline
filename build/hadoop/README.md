# Table of Contents
- [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [1. Build docker image](#1-build-docker-image)
  - [2. Create network](#2-create-network)
  - [3. Run the docker containers](#3-run-the-docker-containers)
  - [4. Create folder for spark user](#4-create-folder-for-spark-user)
  - [5. Accessing the UI](#5-accessing-the-ui)
  - [5. Shutdown Cluster](#5-shutdown-cluster)

## Overview

The Hadoop cluster includes

- HDFS: 1 namenode, 2 datanode
- YARN: 1 resource manager, 2 node manager

## 1. Build docker image

On Linux:

```
docker build -t hadoop:3.3.6 .
```

On Mac using this command:

```
docker build --build-arg ARCH=arm64 -t hadoop:3.3.6 .
```

## 2. Create network

```
docker network create streaming-network --driver bridge
```

## 3. Run the docker containers

Run the docker containers using docker-compose

```
docker compose up -d
```

The output should look like:

```
[+] Running 12/12
 ✔ Volume "hadoop_nodemanager1_data"     Created                                                                                                                                                                         0.0s 
 ✔ Volume "hadoop_nodemanager2_data"     Created                                                                                                                                                                         0.0s 
 ✔ Volume "hadoop_namenode_data"         Created                                                                                                                                                                         0.0s 
 ✔ Volume "hadoop_datanode1_data"        Created                                                                                                                                                                         0.0s 
 ✔ Volume "hadoop_datanode2_data"        Created                                                                                                                                                                         0.0s 
 ✔ Volume "hadoop_resourcemanager_data"  Created                                                                                                                                                                         0.0s 
 ✔ Container hadoop-datanode1-1          Started                                                                                                                                                                         4.1s 
 ✔ Container hadoop-resourcemanager-1    Started                                                                                                                                                                         3.9s 
 ✔ Container hadoop-nodemanager2-1       Started                                                                                                                                                                         4.6s 
 ✔ Container hadoop-namenode-1           Started                                                                                                                                                                         4.2s 
 ✔ Container hadoop-nodemanager1-1       Started                                                                                                                                                                         3.9s 
 ✔ Container hadoop-datanode2-1          Started     
```

Check status of containers

```
docker compose ps
```

The output should look like:

```
NAME                       IMAGE                 COMMAND                  SERVICE           CREATED         STATUS              PORTS
hadoop-datanode1-1         unigap/hadoop:3.3.6   "hdfs datanode"          datanode1         2 minutes ago   Up About a minute   8042/tcp, 8088/tcp, 9000/tcp, 9866-9868/tcp, 9870/tcp, 0.0.0.0:19864->9864/tcp
hadoop-datanode2-1         unigap/hadoop:3.3.6   "hdfs datanode"          datanode2         2 minutes ago   Up About a minute   8042/tcp, 8088/tcp, 9000/tcp, 9866-9868/tcp, 9870/tcp, 0.0.0.0:29864->9864/tcp
hadoop-namenode-1          unigap/hadoop:3.3.6   "/usr/local/bin/star…"   namenode          2 minutes ago   Up About a minute   8042/tcp, 8088/tcp, 9000/tcp, 9864/tcp, 9866-9868/tcp, 0.0.0.0:9870->9870/tcp
hadoop-nodemanager1-1      unigap/hadoop:3.3.6   "yarn nodemanager"       nodemanager1      2 minutes ago   Up About a minute   8088/tcp, 9000/tcp, 9864/tcp, 9866-9868/tcp, 9870/tcp, 0.0.0.0:18042->8042/tcp
hadoop-nodemanager2-1      unigap/hadoop:3.3.6   "yarn nodemanager"       nodemanager2      2 minutes ago   Up About a minute   8088/tcp, 9000/tcp, 9864/tcp, 9866-9868/tcp, 9870/tcp, 0.0.0.0:28042->8042/tcp
hadoop-resourcemanager-1   unigap/hadoop:3.3.6   "yarn resourcemanager"   resourcemanager   2 minutes ago   Up About a minute   8042/tcp, 9000/tcp, 9864/tcp, 9866-9868/tcp, 9870/tcp, 0.0.0.0:8088->8088/tcp
```

## 4. Create folder for spark user

Use the following command to create the `/user/spark` folder on HDFS and grant permissions to the `spark` user. This folder is used when running Spark jobs on Yarn.

```
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -mkdir -p /user/spark && hdfs dfs -chown -R spark:spark /user/spark && hdfs dfs -ls /user/'
```

The output as below:

```
Found 1 items
drwxr-xr-x   - spark spark          0 2025-09-04 08:53 /user/spark
```

## 5. Accessing the UI

The Namenode UI can be accessed at [http://localhost:9870/](http://localhost:9870/) and the ResourceManager UI can be accessed at [http://localhost:8088/](http://localhost:8088/)

## 5. Shutdown Cluster

The cluster can be shut down via:

```shell
docker compose down
```