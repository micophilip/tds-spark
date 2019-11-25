# Top-Down Specialization on Apache Spark&trade;

Proposed top-down specialization algorithm on Apache Spark for COMP 5704

Based on the following papers:
* [A top-down k-anonymization implementation for Apache Spark](https://ieeexplore-ieee-org.proxy.library.carleton.ca/stamp/stamp.jsp?tp=&arnumber=8258492)
* [Top-down specialization for information and privacy preservation](https://ieeexplore-ieee-org.proxy.library.carleton.ca/stamp/stamp.jsp?tp=&arnumber=1410123)

# Build

Run `sbt build`

# Package

Run `sbt package` task for local spark, and `sbt assembly` for Spark Submit

# Run

## Spark submit

### Local

`spark-submit --class TopDownSpecialization --master local[*] target/scala-2.12/code-assembly-0.1.jar <pathToInputDataset> <k>`

### Cluster

`$SPARK_HOME/bin/spark-submit --deploy-mode cluster --master spark://$SPARK_MASTER_HOST:7077 --class TopDownSpecialization --conf spark.sql.shuffle.partitions=$NUM_OF_CORES code-assembly-0.1.jar /home/student/adult-10M.csv 100`

## Cluster Installation

```shell script
sudo apt update -y
# Install Java   
sudo apt install default-jre -y   
sudo apt install default-jdk -y   
echo "JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" | sudo tee /etc/environment  
source /etc/environment

# Install Scala  
sudo apt-get remove scala-library scala   
wget www.scala-lang.org/files/archive/scala-2.12.10.deb   
sudo dpkg -i scala-2.12.10.deb  
sudo apt-get update  
sudo apt-get install scala

# Install Spark   
wget https://archive.apache.org/dist/spark/spark-2.4.2/spark-2.4.2-bin-hadoop2.7.tgz && tar xvf spark-2.4.2-bin-hadoop2.7.tgz  
rm spark-2.4.2-bin-hadoop2.7.tgz  
rm scala-2.12.10.deb  

# If necessary, downgrade Java to v8 (Spark 2.4.2 does not have full support of Java 11)
sudo apt install openjdk-8-jdk -y
sudo update-alternatives --config java
```

## Performance

Performance when doubling datasets and keeping cluster configuration constant. Time increased by 55-65% when dataset increased by 100%

|Records|k|Nodes|Cores|Partitions|Time (in ms)|
|---|---|---|---|---|---|
|250000|100|16|64|64|95352|
|5000000|100|16|64|64|901938|
|10000000|100|16|64|64|1486063|
|20000000|100|16|64|64|2305491|

10-million row speedup test

|nodes|	actual|	optimal|
|---|---|---|
|1|	13723016|	13723016|
|2|	7174442|	6861508|
|4|	3871286|	3430754|
|8|	2103755|	1715377|
|16|	1486063|	857688.5|


# Useful documentation

[Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)    
[Installing Spark Standalone to a Cluster](https://spark.apache.org/docs/latest/spark-standalone.html)