---
title: "Apache Pig: Data Manipulation"
author: "Josep Maria Salvia Hornos"
---

# Apache Pig: Data Manipulation

Apache Pig Tutorial
===================

Introduction
------------

Apache Pig is a high-level data flow language and framework for processing large data sets in Apache Hadoop. It provides an abstraction layer on top of Hadoop that allows data analysts and developers to write complex data processing tasks using a simple, SQL-like scripting language called Pig Latin. Pig Latin is a procedural language that allows users to define data transformations and data processing pipelines, making it easier to perform data analysis on large datasets.

In this tutorial, we will cover the basics of Apache Pig, including installation, data loading, data transformation, and data storing. We will also explore some advanced features of Pig, such as user-defined functions (UDFs) and working with complex data types.

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

Data Manipulation
====================

Data Loading
------------

Apache Pig provides built-in functions to load data from various sources, such as local files, Hadoop Distributed File System (HDFS), Apache HBase, and Apache Cassandra. Here's an example of loading data from a local CSV file:

### Filtering Data
```
-- Load data from a local CSV file
data = LOAD '/path/to/data.csv' USING PigStorage(',')
            AS (name:chararray, age:int, city:chararray);
```

In this example, we use the `LOAD` function to load data from a local CSV file located at `/path/to/data.csv`. We specify the delimiter as ',' using the `PigStorage` function, and define the schema of the data using the `AS` clause, which specifies the field names and their respective data types.

Data Transformation
-------------------

Once the data is loaded, we can perform various data transformations on it using Pig Latin. Pig provides a rich set of built-in functions for data manipulation, filtering, aggregation, and more. Here are some examples of common data transformations:

### Grouping and Aggregating Data
```
-- Filter data based on a condition
filtered_data = FILTER data BY age > 18;
```

In this example, we use the `GROUP` function to group the `data` relation by the `city` field. Then, we use the `FOREACH` function to generate a new relation where we calculate the average age for each city using the `AVG` function.

### Joining Data
```
-- Join data with another relation on a common field
other_data = LOAD '/path/to/other_data.csv' USING PigStorage(',')
                            AS (city:chararray, population:int);
joined_data = JOIN data BY city, other_data BY city;
```

In this example, we use the `JOIN` function to join the `data` relation with another relation `other_data` on the common field `city`. Once the data is joined, we can perform further data transformations on the combined data.

### Data Projection

```
-- Project only specific fields from the data
projected_data = FOREACH data GENERATE name, city;
```
In this example, we use the `FOREACH` function to generate a new relation `projected_data` where we only select the `name` and `city` fields from the `data` relation.

### Sorting Data
```
-- Sort data by age in descending order
sorted_data = ORDER data BY age DESC;
```
In this example, we use the `ORDER` function to sort the `data` relation by the `age` field in descending order.

### Data Cleaning and Transformation
```
-- Clean and transform data using string functions
cleaned_data = FOREACH data GENERATE name, LOWER(city)
    AS city_lower, REPLACE(city, ' ', '_') AS city_cleaned;
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
-----------------

### User-Defined Functions (UDFs)

Apache Pig allows users to define their own custom functions using Java, Python, or other programming languages. These functions are called User-Defined Functions (UDFs) and can be used in Pig Latin scripts to perform custom data transformations. Here's an example of using a UDF in Pig Latin:

```
-- Load data and apply a custom UDF to transform data
REGISTER 'my_udf.jar';
DEFINE my_custom_function com.example.MyCustomFunction();
data = LOAD '/path/to/data.csv' USING PigStorage(',')
            AS (name:chararray, age:int, city:chararray);
transformed_data = FOREACH data GENERATE my_custom_function(name, age)
                                AS transformed_name, city;
```

In this example, we register a custom UDF `my_udf.jar` using the `REGISTER` statement. Then, we define the UDF `my_custom_function` using the `DEFINE` statement, specifying the fully qualified class name of the UDF. Finally, we use the UDF in the `FOREACH` statement to apply the custom transformation to the data.

### Complex Data Types

Apache Pig supports complex data types such as maps, bags, and tuples, which can be used to handle structured and semi-structured data.

#### Tuples

A tuple is an ordered set of fields, which can be of any data type, including other tuples or complex data types. Tuples can be used to represent records or rows in your data.

Example:

```
-- Create a tuple
person = ('John', 30, 'New York');
```

#### Bags

A bag is an unordered collection of tuples. Bags can be used to represent a group of records or a set of rows.

Example:

```
-- Create a bag
people = {('John', 30, 'New York'), ('Alice', 28, 'San Francisco'), ('Bob', 32, 'Chicago')};
```

#### Maps

A map is a collection of key-value pairs, where the keys must be of type `chararray` and the values can be of any data type, including complex data types. Maps can be used to represent data with dynamic schemas or sparse data.

Example:

```
-- Create a map
person_map = ['name' # 'John', 'age' # 30, 'city' # 'New York'];
```

### Working with Complex Data Types

To work with complex data types in Pig Latin, you can use built-in functions and operators to access, manipulate, and transform the data.

#### Accessing Complex Data Types

To access fields in a tuple, use the `$` notation followed by the field index (starting from 0):

```-- Access the first field (name) of a tuple
name = person.$0;
```

To access values in a map, use the `#` notation followed by the key:

```-- Access the 'name' value in a map
name = person_map#'name';
```

To access elements in a bag, you can use the `FLATTEN` function or nested `FOREACH` statements.

#### Manipulating and Transforming Complex Data Types

You can use built-in functions and operators to manipulate and transform complex data types in your Pig Latin scripts.

For example, you can use the `TOTUPLE` function to create a tuple from individual fields, the `TOBAG` function to create a bag from tuples, and the `TOMAP` function to create a map from key-value pairs.

```-- Create a tuple, bag, and map using built-in functions
new_person = TOTUPLE('Jane', 25, 'Los Angeles');
new_people = TOBAG(new_person, ('John', 30, 'New York'));
new_person_map = TOMAP('name', 'Jane', 'age', 25, 'city', 'Los Angeles');
```

You can also use the `CONCAT` function to concatenate two maps, the `UNION` function to merge two bags, and the `CROSS` function to create a Cartesian product of two bags.

```-- Merge maps, bags, and create a Cartesian product of bags
merged_maps = CONCAT(person_map, new_person_map);
merged_people = UNION(people, new_people);
people_cross = CROSS(people, new_people);
```

# Complete Example

Suppose you are a data analyst working for a chain of supermarkets that operates in multiple cities. The management wants to understand the demographics of their adult customer base (age > 18) in order to make informed decisions on targeted marketing campaigns and product offerings. Specifically, they are interested in knowing the average age of adult customers in each city, along with the total population of each city, to identify potential growth opportunities.

You are provided with two datasets: one containing customer information (name, age, city), and another containing the population of each city. The management requires the data to be cleaned and sorted by city, with the average age of adult customers presented in a specific format (using a comma instead of a decimal point) and the city names capitalized.

In this scenario, the given Apache Pig script will help you achieve the desired output by processing and combining the two datasets, filtering out customers aged 18 or below, aggregating the data, and applying the required transformations. The cleaned and sorted data can then be used by the management to make strategic decisions on marketing and expansion.


```
-- Load data from a CSV file
data = LOAD '/path/to/data.csv' USING PigStorage(',')
        AS (name:chararray, age:int, city:chararray);

-- Filter data to get only records with age greater than 18
filtered_data = FILTER data BY age > 18;

-- Group data by city
grouped_data = GROUP filtered_data BY city;

-- Calculate average age for each city
aggregated_data = FOREACH grouped_data GENERATE group
            AS city, AVG(filtered_data.age) AS avg_age;

-- Join data with another dataset
other_data = LOAD '/path/to/other_data.csv' USING PigStorage(',')
                            AS (city:chararray, population:int);
joined_data = JOIN aggregated_data BY city, other_data BY city;

-- Project only specific fields from the joined data
projected_data = FOREACH joined_data GENERATE aggregated_data::city
                                    AS city, avg_age, population;

-- Sort data by city in ascending order
sorted_data = ORDER projected_data BY city ASC;

-- Clean and transform data using string functions
cleaned_data = FOREACH sorted_data GENERATE city,
        (chararray)REPLACE((chararray)avg_age, '\\.', ',')
        AS avg_age, UPPER(city) AS city_upper;

-- Store cleaned data to a local CSV file
STORE cleaned_data INTO '/path/to/cleaned_data.csv' USING PigStorage(',');

```

In this example, we first load data from a CSV file using `LOAD` and specify the schema with `AS` to define the fields. Then, we apply filters with `FILTER` to get only records that meet a specific condition. Next, we use `GROUP` to group the data by a certain field, and then use `FOREACH` and aggregation functions such as `AVG` to perform calculations on each group.


We also demonstrate how to join data from multiple datasets using `JOIN`, and how to project only specific fields from the joined data using `FOREACH`. We use `ORDER` to sort the data by a field in ascending or descending order. Additionally, we show how to clean and transform data using string functions such as `REPLACE` and `UPPER`, and how to store the cleaned data to a local CSV file using `STORE` and `PigStorage`.

Note: This is just a simple example and Apache Pig provides many more advanced features, including support for complex data types, nested queries, parameter substitution, and debugging capabilities.

1.  data.csv:

```
Alice,22,New York
Bob,18,Los Angeles
Charlie,25,Chicago
David,20,New York
Eve,28,Los Angeles
Frank,30,Chicago
Grace,19,New York
Hannah,21,Los Angeles
Isaac,23,Chicago
Jack,11,New York
Karl,12,Los Angeles
Lily,17,Chicago
Mia,30,New York
Nathan,29,Los Angeles
Olivia,28,Chicago
Penny,18,New York
Quinn,19,Los Angeles
Riley,20,Chicago
Sarah,21,New York
Tom,22,Los Angeles
```

2.  other_data.csv:

```
New York,8537673
Los Angeles,39776830
Chicago,2722389
```

You can save these files with the respective names and contents to your local filesystem, and then use the file paths in the Apache Pig script to load the data and perform the operations as shown in the tutorial example. Make sure that the file paths in the script match the actual locations of the input files on your system.

Note: The format of the input files should match the schema specified in the `LOAD` statement of the Apache Pig script, which in the example is comma-separated values (CSV) with three fields: name (chararray), age (int), and city (chararray) for data.csv, and city (chararray) and population (int) for other_data.csv. If your input files have a different format or schema, you may need to modify the script accordingly.

Apache Pig: Internal Working and Distributed Computation
=========================================================

Apache Pig is designed to handle large-scale data processing tasks and is capable of distributed computation across a cluster of machines. It follows a high-level data flow language that allows users to express complex data transformations in a simple and concise manner, abstracting the complexities of distributed computing.

### Execution Model

Apache Pig follows a two-stage execution model: the logical plan and the physical plan.

1.  **Logical Plan**: When a Pig script is submitted, Apache Pig parses the script and generates a logical plan, which is a directed acyclic graph (DAG) representation of the operations to be performed on the data. The logical plan represents the sequence of transformations and operations that need to be applied to the input data to produce the desired output.

2.  **Physical Plan**: The logical plan is then optimized and converted into a physical plan, which specifies how the logical operations are to be executed on the cluster. The physical plan is optimized for distributed processing and includes details such as the distribution of data across the nodes in the cluster, the order of operations, and the use of combiners and reducers for aggregation.

### Distributed Computation

Apache Pig uses a distributed computing model to process large datasets in parallel across a cluster of machines. The data is divided into smaller chunks called "splits" or "blocks", which are processed independently on different nodes in the cluster. Each node executes a portion of the physical plan on its local data and produces intermediate results.

Apache Pig uses Apache Hadoop as the underlying execution engine for distributed processing. It leverages Hadoop's HDFS (Hadoop Distributed File System) for storing and managing large datasets, and Apache MapReduce for distributed processing. Pig translates the logical and physical plans into MapReduce jobs that are executed on the cluster.

### Data Flow and Pipelining

One of the key features of Apache Pig is its data flow and pipelining capability. Pig allows users to express complex data transformations using a sequence of operations, where the output of one operation becomes the input to the next operation in a pipeline. This allows for efficient data processing as intermediate results can be passed between operations without writing the intermediate data to disk, reducing I/O overhead.

Apache Pig optimizes the data flow and pipelining by grouping operations that can be executed together and minimizing data movement across the cluster. This is achieved through Pig's logical and physical optimization techniques, such as pushdown predicates, filter and projection pushdown, and combining multiple operations into a single MapReduce job.

### Load Balancing and Fault Tolerance

Apache Pig provides load balancing and fault tolerance mechanisms to ensure reliable and efficient distributed processing.

1.  **Load Balancing**: Pig dynamically balances the workload across the cluster by evenly distributing the data blocks across the available nodes. This ensures that the processing is evenly distributed, and no single node becomes a bottleneck.

2.  **Fault Tolerance**: Pig provides fault tolerance through data replication in HDFS. Data blocks are replicated across multiple nodes to ensure data durability and availability even in the case of node failures. If a node fails during processing, Pig automatically re-routes the failed tasks to other available nodes to ensure job completion.

Summary
===============

Apache Pig is a powerful distributed data processing framework that abstracts the complexities of distributed computing through its logical and physical plans. It leverages Hadoop's HDFS and MapReduce for distributed processing, and provides features such as data flow and pipelining, load balancing, and fault tolerance to ensure efficient and reliable processing of large datasets in a distributed environment.