## Movie Rating Analysis using PySpark
This is a simple project that demonstrates how to use Apache Spark to analyze movie ratings data. The project uses Spark's DataFrame API to load and process data from two CSV files: one containing movie ratings data and another containing movie metadata.

#### The project performs the following tasks:

* Load movie ratings data from a CSV file into a Spark DataFrame.
* Load movie metadata from a CSV file into a separate Spark DataFrame.
* Join the two DataFrames to create a single DataFrame containing both rating and metadata information.
* Filtered out top rated movies (whose ratings are greater then 8)
* Display the top-rated movies.
* Sort the results by rating in descending order.
* Save the data in the local file system as Parquet file.

## Technologies Used
#### The following technologies were used to implement this project:

* PySpark: A fast and general-purpose distributed computing system.
* Python: A high-level programming language.

## Original Output
#### The following is a original output of the project:

```
+--------+---------------------------+-------+
|movie_id|movie_title                |ratings|
+--------+---------------------------+-------+
|1       |RRR                        |8      |
|2       |The Kashmiri Files         |8.3    |
|3       |KGF Chapter 2              |8.4    |
|4       |Vikram                     |8.4    |
|5       |Kantara                    |8.6    |
|6       |Rocketry: The Nambi Effect |8.8    |
|7       |Major                      |8.2    |
|8       |Sita Ramam                 |8.6    |
|9       |Ponniyin Selvan : Part One |7.9    |
|10      |777 Charlie                |8.9    |
|11      |Lal Singh Chadda           |5.5    |
|12      |Gangubai Kathiawadi        |7.8    |
|13      |Beast                      |5.2    |
|14      |Karthikeya 2               |8      |
|15      |Radhe Shyam                |5.3    |
|16      |Hridayam                   |8.1    |
|17      |DJ Tillu                   |7.2    |
|18      |Love Mocktail 2            |7.5    |
|19      |Brahmastra Part One : Shiva|5.5    |
|20      |FIR                        |6.9    |
+--------+---------------------------+-------+

```
## Filtered Output
#### The following is a filtered output of the project:

```
+--------+--------------------------+-------+
|movie_id|movie_title               |ratings|
+--------+--------------------------+-------+
|1       |RRR                       |8      |
|2       |The Kashmiri Files        |8.3    |
|3       |KGF Chapter 2             |8.4    |
|4       |Vikram                    |8.4    |
|5       |Kantara                   |8.6    |
|6       |Rocketry: The Nambi Effect|8.8    |
|7       |Major                     |8.2    |
|8       |Sita Ramam                |8.6    |
|10      |777 Charlie               |8.9    |
|14      |Karthikeya 2              |8      |
|16      |Hridayam                  |8.1    |
+--------+--------------------------+-------+

```
## Sorted Output
#### The following is a sorted output of the project:

```
+--------+--------------------------+-------+
|movie_id|movie_title               |ratings|
+--------+--------------------------+-------+
|10      |777 Charlie               |8.9    |
|6       |Rocketry: The Nambi Effect|8.8    |
|5       |Kantara                   |8.6    |
|8       |Sita Ramam                |8.6    |
|3       |KGF Chapter 2             |8.4    |
|4       |Vikram                    |8.4    |
|2       |The Kashmiri Files        |8.3    |
|7       |Major                     |8.2    |
|16      |Hridayam                  |8.1    |
|1       |RRR                       |8      |
|14      |Karthikeya 2              |8      |
+--------+--------------------------+-------+
```

## Output Data in Parquet Format

PAR1 €t, 6 (81   @   ,10   6   5 8 3 4 2 7),6   1   14 ÒÖ, 6 (Vikram
777 Charlie   ©ðR   
   777 Charlie   Rocketry: The Nambi Effect    Kantara
   Sita Ramam
   KGFFLpter 2   Vikram   HäKashmiri Files   Major   Hridayam   RRR   Karthikeya 2lTL  6   8.9  8	  6	  4	  3	 02   8.1   8  ,6 (8.98   4   ˆ´±þ LHspark_schema %movie_id%  %
movie_title%  % ratings%  <&5 movie_id¸¬&<6 (81      &´5 
movie_titleª®&´<6 (Vikram
777 Charlie      &â5 ratingsÜÈ&â<6 (8.98 ,     ¾ ,org.apache.spark.version2.4.7 )org.apache.spark.sql.parquet.row.metadataä{"type":"struct","fields":[{"name":"movie_id","type":"string","nullable":true,"metadata":{}},{"name":"movie_title","type":"string","nullable":true,"metadata":{}},{"name":"ratings","type":"string","nullable":true,"metadata":{}}]} Jparquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1)<       ¥  PAR1
```
