# Agens Spark Connector 

Agens Connector for Apache Spark based on ES-Hadoop

## build

```shell script
mvn clean package -DskipTests
```

## run

```shell script
## for TEST
spark-submit --executor-memory 1g \
    --master spark://minmac:7077 \
    --class net.bitnine.agens.spark.Agens \
    target/agens-spark-connector-1.0-dev.jar

## for IMPORT
```
