# Top-Down Specialization on Apache Spark&trade;

Proposed top-down specialization algorithm on Apache Spark for COMP 5704

# Build

Run `sbt build`

# Package

Run `sbt package` task for local spark, and `sbt assembly` for Spark Submit

# Run

## Spark submit

`spark-submit --class TopDownSpecialization --master local target/scala-2.12/code_2.12-0.1.jar <pathToInputDataset> <pathToTaxonomyTree> <k>`

## Performance

|Records|k|Nodes|Cores|Partitions|Time (in ms)|
|---|---|---|---|---|---|
|32562|100|1|1|1|240744|
|32562|100|1|8|1|81978|
|32562|100|1|8|8|34863|
|32562|100|1|16|16|44157|
|32562|100|1|16|8|41208|
|32562|100|1|16|4|42621|
|32562|50|1|8|8|55188|
|250000|100|1|8|8|161497|
|250000|100|1|16|32|162399|


# Useful documentation

[Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)  
[Set up Apache Spark on a Multi-Node Cluster](https://medium.com/ymedialabs-innovation/apache-spark-on-a-multi-node-cluster-b75967c8cb2b)  
[Installing Spark Standalone to a Cluster](https://spark.apache.org/docs/latest/spark-standalone.html)