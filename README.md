Interview Challenge
========

To get started, download and install Scala 2.10, sbt, and Spark 1.6.1.

## Download and install Spark

Download the prebuilt version 1.6.1 from here: [Download Spark](http://spark.apache.org/downloads.html)  
Move it to the standard installation directory on your machine.  
Set the `$SPARK_HOME` environment variable to this directory.

# Compilation

To build from source, execute the package command from sbt:

```
challenge.git$ sbt package
```

# Input files

Copy the OANC input transcripts into the `resources` directory. The expected path is:

```
resources/OANC-GrAF/data/spoken/telephone/switchboard
```

# Execution

To generate output files, run the jar you just created in standalone mode. This will run locally on a single machine.

```
challenge.git$ $SPARK_HOME/spark-submit target/scala-2.10/interview-challenge_2.10-1.0.jar
```

# Results

The relevant output files can be found here:

```
output/feature1.txt  
output/feature2.txt
```

# Scalability

While this sample code runs on a single node, the driver could easily be modified to operate on a full Spark cluster, whether standalone or Hadoop-based. Provided the input files are sufficiently small to fit into memory (and thus suitable for use with `sc.wholeTextFiles()`), this solution should scale well with the addition of file consolidation functionality.
