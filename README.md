# Top-Down Specialization on Apache Spark&trade;

Proposed top-down specialization algorithm on Apache Spark for COMP 5704

# Build

Run `build.sbt`

# Package

Run `sbt package` task

# Run

## Spark submit

`spark-submit --class TopDownSpecialization --master local target/scala-2.12/code_2.12-0.1.jar <pathToInputDataset>`