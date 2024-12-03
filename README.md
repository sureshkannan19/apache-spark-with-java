# Apace Spark - Learning
To understand the need for Spark, lets start with Hadoop.
<hr style="border: 3px solid;">

# Native Hadoop features: 
* **HDFS** (Hadoop Distributed File System): It acts as a **distributed storage layer**. 
  HDFS is great for storing large-scale data but **does not have querying capabilities**.
* **MapReduce**: A distributed data processing framework that operates over the data stored in HDFS only. 
* **YARN** (Yet Another Resource Manager): YARN as the cluster manager, allocates resources (CPU, memory) 
 to worker nodes and schedules jobs.

# Spark:
* Spark is a big data processing framework works in distributed environment like Hadoop's MapReduce, and its API provides a robust way
  to handle large-scale data efficiently.
* It's built on Scala Programming language.

### Spark Key features:
* Batch/streaming data using Spark Streaming
* SQL analytics using RDD's and SparkSQL (querying distributed data)
* Machine Learning.
* Graph processing.

<hr style="border: 3px solid;">

## Spark Vs Hadoop's MapReduce: 
Although both Spark and MapReduce does data processing, the way its done is the chief difference.
* **Speed**: Spark can perform operations up to 100X faster than MapReduce because MapReduce writes most of the **data to disk**
  after each map and reduce operation; however Spark keeps most of the data **in memory** after each transformation.
  Spark will write to disk only when the memory is full.
* **Flexible Storage system**: MapReduce requires files to be stored only in HDFS, while
    Spark can work on data stored in a variety of formats like **HDFS, AWS S3, Cassandra, HBase** etc.
*  **Querying Capabilities**: Hadoop natively doesn't support querying data, hence tools such as
    **Hive, Hbase, Pig, SparkSQL**  is built on top of HDFS to provide querying features
* **SparkSql - DataFrame API** : a distributed collection of data organized into named columns, resembling a table in a relational database.
* **Real-time analytics**: Spark is effective when compared to Hadoop, because Hadoop persist to disk to store 
  intermittent result of map and reduce operations, which results in lot of I/O operations,
  hence hadoop not good for real-time and iterative processes.

* Detailed explanation on Hadoop MapReduce Vs Spark (link to integrate.io ): <a href="https://www.integrate.io/blog/apache-spark-vs-hadoop-mapreduce/">Spark Vs Hadoop</a>
<hr style="border: 3px solid;">

## Hadoop role in Spark:
* Although Hadoop plays an optional role in the Apache Spark ecosystem, it primarily used as a **resource provider(YARN)** to Spark Jobs.

## Installation & Setup:
* Step 1: Download <a href="https://spark.apache.org/downloads.html">Spark - Hadoop</a> binaries.
* Step 2: For Hadoop binaries to work in windows, Download **winutils.exe** and **hadoop.dll** files from <a href="https://github.com/cdarlint/winutils">repo.</a>
* Step 3: After downloading, place **winutils.exe** and **hadoop.dll** in **spark-3.5.3-bin-hadoop3\bin** path (from Step 1).
* For detailed installation and explanation of Spark in Java, check out <a href="https://github.com/backstreetbrogrammer/11_JavaSpark#23-download-winutilsexe-and-hadoopdll-only-for-windows">this repo</a>

## JavaSparkContext:
* It is the entry point for a Spark application.

**Key Responsibilities:**
* **Cluster Communication**: Connects with the cluster manager (YARN, Mesos, Kubernetes or Spark Standalone) to request resources for executing tasks.
* **Job Scheduling**: Breaks down a Spark application into stages and tasks, then schedules their execution across the cluster.
* **RDD Creation**: Creates RDDs from external data sources (e.g., HDFS, S3, local file systems).
Broadcast Variables and Accumulators: Manages shared variables used across nodes.

## Spark RDD:
* **RDD**(Resilient Distributed Dataset) is the fundamental data abstraction in Spark.
* It represents an **immutable** distributed collection of objects that can be processed in parallel across the cluster.
* **Fault-tolerant**: meaning they can automatically recover lost partitions due to node failures.
**Lazy Evaluation:** Transformations on RDDs are not executed immediately. They are only computed when an action is triggered.