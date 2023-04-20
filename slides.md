---
title: "Apache Pig: Data Manipulation"
author: "Josep Maria Salvia Hornos"
---

Introduction
------------

Apache Pig is a high-level data flow language and framework for processing large data sets in Apache Hadoop. It provides an abstraction layer on top of Hadoop that allows data analysts and developers to write complex data processing tasks using a simple, SQL-like scripting language called Pig Latin.

Pig Latin
------------

Pig Latin is a procedural language that allows users to define data transformations and data processing pipelines, making it easier to perform data analysis on large datasets.

In this tutorial, we will cover the basics of Apache Pig, including installation, data loading, data transformation, and data storing.

Prerequisites
-------------

Before you start with Apache Pig, you should have the following prerequisites in place:

1.  Java Development Kit (JDK) installed
2.  Hadoop cluster or a standalone Hadoop installation
3.  Pig installed on your system (downloadable from the Apache Pig website)

Installation
------------

To install Pig on your system, follow these steps:

1.  Download the latest stable release of Pig from the Apache Pig website (<https://pig.apache.org/>).
2.  Extract the downloaded archive to a directory of your choice.
3.  Set the `PIG_HOME` environment variable to the path of the extracted directory.
4.  Add the Pig binary directory to your system's `PATH` variable.

Data Maniuplation
====================

Data Loading
------------

Apache Pig provides built-in functions to load data from various sources, such as local files, Hadoop Distributed File System (HDFS), Apache HBase, and Apache Cassandra.

### Filtering Data
```
-- Load data from a local CSV file
data = LOAD '/path/to/data.csv'
    USING PigStorage(',')
    AS (name:chararray, age:int, city:chararray);
```

Grouping and Aggregating Data
-------------------

```
-- Filter data based on a condition
filtered_data = FILTER data BY age > 18;
```

In this example, we use the `GROUP` function to group the `data` relation by the `city` field. Then, we use the `FOREACH` function to generate a new relation where we calculate the average age for each city using the `AVG` function.

Joining Data
-------------------

```
-- Join data with another relation
other_data = LOAD '/path/to/other_data.csv'
    USING PigStorage(',')
    AS (city:chararray, population:int);
joined_data = JOIN data BY city,
            other_data BY city;
```

In this example, we use the `JOIN` function to join the `data` relation with another relation `other_data` on the common field `city`.

Data Projection
-------------------

```
-- Project only specific fields from the data
projected_data = FOREACH data GENERATE name, city;
```
In this example, we use the `FOREACH` function to generate a new relation `projected_data` where we only select the `name` and `city` fields from the `data` relation.

Sorting Data
-------------------

```
-- Sort data by age in descending order
sorted_data = ORDER data BY age DESC;
```
In this example, we use the `ORDER` function to sort the `data` relation by the `age` field in descending order.

Data Cleaning and Transformation
-------------------

```
-- Clean and transform data
cleaned_data = FOREACH data
               GENERATE name,
               LOWER(city)
    AS  city_lower,
        REPLACE(city, ' ', '_')
    AS city_cleaned;
```

In this example, we use various string functions such as `LOWER` and `REPLACE` to clean and transform the data. We generate a new relation `cleaned_data` where we select the `name` field as it is, convert the `city` field to lowercase using the `LOWER` function, and replace spaces with underscores in the `city` field using the `REPLACE` function.


Data Storing
------------

Once the data is transformed, we can store the results back to a file, HDFS, or other supported storage systems. Here's an example of storing the data to a local CSV file:

```
-- Store data to a local CSV file
STORE cleaned_data INTO '/path/to/cleaned_data.csv' USING PigStorage(',');
```

In this example, we use the STORE function to store the cleaned_data relation to a local CSV file located at /path/to/cleaned_data.csv. We specify the delimiter as ',' using the PigStorage function.


Advanced Features
-------------------

Apache Pig provides many more advanced features, including support for complex data types, nested queries, parameter substitution, and debugging capabilities.


Demo
=======

Suppose you are a data analyst working for a chain of supermarkets that operates in multiple cities. The management wants to understand the demographics of their adult customer base (age > 18) in order to make informed decisions on targeted marketing campaigns and product offerings. Specifically, they are interested in knowing the average age of adult customers in each city, along with the total population of each city, to identify potential growth opportunities.

Dataset
-------------------

You are provided with two datasets: one containing customer information (name, age, city), and another containing the population of each city. The management requires the data to be cleaned and sorted by city, with the average age of adult customers presented in a specific format (using a comma instead of a decimal point) and the city names capitalized.




Internal Working and Distributed Computation
=========================================================

Apache Pig is designed to handle large-scale data processing tasks and is capable of distributed computation across a cluster of machines. It follows a high-level data flow language that allows users to express complex data transformations in a simple and concise manner, abstracting the complexities of distributed computing.

Execution Model
-----------------

Apache Pig follows a two-stage execution model: the logical plan and the physical plan.

1.  **Logical Plan**: When a Pig script is submitted, Apache Pig parses the script and generates a logical plan, which is a directed acyclic graph (DAG) representation of the operations to be performed on the data. The logical plan represents the sequence of transformations and operations that need to be applied to the input data to produce the desired output.

2.  **Physical Plan**: The logical plan is then optimized and converted into a physical plan, which specifies how the logical operations are to be executed on the cluster. The physical plan is optimized for distributed processing and includes details such as the distribution of data across the nodes in the cluster, the order of operations, and the use of combiners and reducers for aggregation.

Distributed Computation
-------------------------

Apache Pig uses a distributed computing model to process large datasets in parallel across a cluster of machines. The data is divided into smaller chunks called "splits" or "blocks", which are processed independently on different nodes in the cluster. Each node executes a portion of the physical plan on its local data and produces intermediate results.

Apache Pig uses Apache Hadoop as the underlying execution engine for distributed processing. It leverages Hadoop's HDFS (Hadoop Distributed File System) for storing and managing large datasets, and Apache MapReduce for distributed processing. Pig translates the logical and physical plans into MapReduce jobs that are executed on the cluster.

Data Flow and Pipelining
-------------------------

One of the key features of Apache Pig is its data flow and pipelining capability. Pig allows users to express complex data transformations using a sequence of operations, where the output of one operation becomes the input to the next operation in a pipeline. This allows for efficient data processing as intermediate results can be passed between operations without writing the intermediate data to disk, reducing I/O overhead.

Apache Pig optimizes the data flow and pipelining by grouping operations that can be executed together and minimizing data movement across the cluster. This is achieved through Pig's logical and physical optimization techniques, such as pushdown predicates, filter and projection pushdown, and combining multiple operations into a single MapReduce job.

Load Balancing and Fault Tolerance
-----------------------------------

Apache Pig provides load balancing and fault tolerance mechanisms to ensure reliable and efficient distributed processing.

1.  **Load Balancing**: Pig dynamically balances the workload across the cluster by evenly distributing the data blocks across the available nodes. This ensures that the processing is evenly distributed, and no single node becomes a bottleneck.

2.  **Fault Tolerance**: Pig provides fault tolerance through data replication in HDFS. Data blocks are replicated across multiple nodes to ensure data durability and availability even in the case of node failures. If a node fails during processing, Pig automatically re-routes the failed tasks to other available nodes to ensure job completion.

Summary
===============

Apache Pig is a powerful distributed data processing framework that abstracts the complexities of distributed computing through its logical and physical plans. It leverages Hadoop's HDFS and MapReduce for distributed processing, and provides features such as data flow and pipelining, load balancing, and fault tolerance to ensure efficient and reliable processing of large datasets in a distributed environment.