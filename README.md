# Creating a Data Lake on AWS
**Creating a Data Lake for building a relational database on AWS**
 
### Follwoing are the used libraries
configparser, datetime, os, pyspark

If the script is run directly on a emr-cluster the configparser is not used.

### Project Motivation
A startup called Sparkify, needs to have a relational database in the form of a star-schema to improve the data-analysis. The provided data consists of json files, which need to be reordered into five new tables.
Below are the tables, which were created during the project: 

<ins>Fact table:</ins> songplays

<ins>Dimension tables:</ins> users, songs, artists, times

![alt text](https://github.com/riconaef/Data_Lake/blob/main/process_data.png)

The data is loaded from an S3 bucket on AWS to the emr-cluster. From there the data are reordered with the help of an ETL pipeline into 5 new tables which have an star-schema architecture. Compared to a Data Warehouse, the data is not transformed thus it is not a typical etl pipeline.

### File Descriptions
etl.py<br />
df.cfg<br />

The "etl.py" can be run, which loads the data from the S3 bucket, then loads the data back on another S3 bucket. 
If the script is run on a notebook, the main function can be called as follows: main().

### Test query
To test, the data are loaded back in from the parquet files to perform a test query. 

test = spark.sql("""<br />
<&emsp> SELECT so.title, COUNT(*) amount<br />
    FROM df_songs so<br />
    JOIN df_songplay sp ON (so.song_id = sp.song_id)<br />
    GROUP BY so.title""").show()<br />
 
Output:<br />
![alt text](https://github.com/riconaef/Creating-a-Data-Warehouse-on-AWS/blob/main/query1.png)

### Licensing, Authors, Acknowledgements
I thank Sparkify for offering the data.
