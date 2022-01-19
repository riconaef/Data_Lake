# Creating-a-Data_Lake-on-AWS
**Creating a Data Warehouse for building a relational database with Postgres on AWS**
 
### Follwoing are the used libraries
configparser, psycopg2

### Project Motivation
A startup called Sparkify, needs to have a relational database in the form of a star-schema to improve the data-analysis. The provided data consists of json files, which need to be reordered into five new tables.
Below are the tables, which were created during the project: 

<ins>Staging tables:</ins> staging_events_table, staging_songs_table

<ins>Fact table:</ins> songplay

<ins>Dimension tables:</ins> users, songs, artists, times

![alt text](https://github.com/riconaef/Creating-a-Data-Warehouse-on-AWS/blob/main/star-schema.png)

The data is loaded from an S3 storage on AWS into the staging tables. From there the data are reordered with the help of an ETL pipeline into 5 new tables which have an star-schema architecture. 

### File Descriptions
sql_queries.py<br />
create_tables.py<br />
etl.py<br />
dwh.cfg<br />

To run, first the "create_tables.py" needs to be run, which creates the tables. After, "etl.py" can be run, which fills the table with an etl-pipeline. 

### Test query
SELECT songs.title, COUNT(*) amount<br />
FROM songs<br />
JOIN songplay ON (songs.song_id = songplay.song_id)<br />
GROUP BY songs.title<br />
ORDER BY amount DESC<br />
LIMIT 10<br />
 
Output:<br />
![alt text](https://github.com/riconaef/Creating-a-Data-Warehouse-on-AWS/blob/main/query1.png)

### Licensing, Authors, Acknowledgements
I thank Sparkify for offering the data.
