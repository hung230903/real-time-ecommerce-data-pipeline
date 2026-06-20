# Table of Contents
- [Table of Contents](#table-of-contents)
  - [1. Overview](#1-overview)
  - [2. Setup Hadoop](#2-setup-hadoop)
  - [3. Build spark docker image](#3-build-spark-docker-image)
  - [4. Create volumes](#4-create-volumes)

## 1. Overview

Installing and running Spark programs on Hadoop Yarn.

## 2. Setup Hadoop

See installation instructions at [Hadoop Installing](../hadoop/README.md)

## 3. Build spark docker image

- Build a custom spark image. 
- This image comes pre-installed with programs like Miniconda, etc., used to submit Spark jobs that run on Yarn.

```shell
docker build -t unigap/spark:3.5 .
```

## 4. Create volumes

Create volumes to store data and cache Spark libraries.

```shell
docker volume create spark_data
docker volume create spark_lib
```