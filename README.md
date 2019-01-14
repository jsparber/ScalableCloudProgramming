# Project for Scalable and Cloud Programming 2018/19 at UNIBO
This project is based on [TwitterPopularTags](https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala) and [Spark-Docker-Example](https://github.com/Semantive/Spark-Docker-Example)

## Requirements
1. [Docker](https://docs.docker.com/engine/installation/) 1.10.0+ and [docker-compose](https://docs.docker.com/compose/) 1.6.0+
2. [SBT](http://www.scala-sbt.org/)

## Running
1. Create docker image of the sbt-application by executing following command inside ``sbt-application`` directory:

```sbt docker```

2. Run whole cluster using ```docker-compose``` (from the directory containing ```docker-compose.yml``` file):

```docker-compose up```

Docker-compose will run three images: 
- Spark master
- Spark worker
- sbt-application (Spark job)
