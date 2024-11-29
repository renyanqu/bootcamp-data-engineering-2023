[![Tests](https://barrelsofdata.com/api/v1/git/action/status/fetch/barrelsofdata/spark-boilerplate/tests)](https://git.barrelsofdata.com/barrelsofdata/spark-boilerplate/actions?workflow=workflow.yaml)
[![Build](https://barrelsofdata.com/api/v1/git/action/status/fetch/barrelsofdata/spark-boilerplate/build)](https://git.barrelsofdata.com/barrelsofdata/spark-boilerplate/actions?workflow=workflow.yaml)

# Spark Boilerplate
This is a boilerplate project for Apache Spark. The related blog post can be found at [https://www.barrelsofdata.com/spark-boilerplate-using-scala](https://www.barrelsofdata.com/spark-boilerplate-using-scala)

## Build instructions
From the root of the project execute the below commands
- To clear all compiled classes, build and log directories
```shell script
./gradlew clean
```
- To run tests
```shell script
./gradlew test
```
- To build jar
```shell script
./gradlew build
```

## Run
```shell script
 bin/spark-submit \
 --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,software.amazon.awssdk:bundle:2.20.18 \
 --class com.dataengineer.io.examples.NbaPlayerTransformationJob ../../4-apache-spark-bonus-scala/build/libs/datasetapi-example.jar
```