# Table of Contents
- [Table of Contents](#table-of-contents)
  - [1. Create network](#1-create-network)
  - [2. Run postgres](#2-run-postgres)
  - [3. Monitor](#3-monitor)
  - [References](#references)

## 1. Create network

```shell
docker network create streaming-network --driver bridge
```

## 2. Run postgres

```shell
docker compose up -d
```

**Check Status & Logs**

```shell
docker compose ps
docker compose logs postgres -f -n 100
```

## 3. Monitor

Access the `adminer` address and enter the `postgres` information (See `environment` in [Docker Compose](./docker-compose.yml)).

[adminer](http://localhost:8380)

**Note:** `Adminer` is just a tool to connect to the database; we can use other tools such as `pgAdmin`, `DBeaver`, etc.

## References

[Postgres Docker Image](https://hub.docker.com/_/postgres)