# PROJECT:  DATA WAREHOUSE

### Project Summary: 
Sparkify is a music streaming app who wants to analyze the user activity and song details through their streaming app. The data regarding what users are listening to is being sent in json format. Since the logs of user activity and metadata of songs are present in json format, it is difficult for them to query. 
I would like to create a database for this work called dwh in Redshift and create tables for the songs and user activity. I have used AWS REDSHIFT and build a ETL pipeline using python.
            
**Songs Dataset Example:**
Song dataset contains metadata about the artist and the songs they composed.

**Song Data S3 link:** s3://udacity-dend/song_data

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


**Logs Dataset Example:**
Logs dataset contains activity of the users from the streaming app based on certain configurations.

**Log Data S3 link:** s3://udacity-dend/log_data

The json contains details regarding the users such as firstname, lastname, gender, artists, songs, length of the song, starttime, userid.


**FACT Table:**

> **songplays:** records in log data associated with song plays 

>> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables:**

> **users:** users in the app
>>    userId, firstName, lastName, gender, level

> **songs:** songs in music database
>>    song_id, title, artist_id, year, duration

>**artists:** artists in music database
>>    artist_id, name, location, latitude, longitude

>**time:** timestamps of records in songplays broken down into specific units
>>    start_time, hour, day, week, month, year, weekday


**Staging Tables:**
>**staging_events** 
>>    artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

>**staging_songs**
>>    num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year
    
### Python Script:
>**PROJECT.ipynb**
   1. Added the CLUSTER, IAM and S3 details into dwh.cfg file.
   2. Loaded the parameters from the dwh.cfg file into a dataframe.
   3. Created clients for S3, IAM  and Redshift. Also created the IAM role and attached the policy.
   4. !python create_tables.py *(used to call create_tables.py script)*

>**create_tables.py**
   1. CREATE statements are written in sql_queries.py to create each table.
   2. DROP statements are written in sql_queries.py to drop each table if it exists.
   3. Run !python create_tables.py to drop and create tables.

>**etl.py**
   1. Copy and insert statements are  written in sql_queries.py.
   2. Run !python etl.py to copy data from S3 bucket into the staging tables and insert data into Dimension and         Fact tables.
   3. Verify by using Query Editor in AWS Redshift console.
   
   
>**Delete Cluster**

Deleted the cluster and detach the IAM role.


