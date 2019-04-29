# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Capstone Project: Parsing Nested Data
# MAGIC 
# MAGIC Mount JSON data using DBFS, define and apply a schema, parse fields, and save the cleaned results back to DBFS.
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Please use a <a href="https://docs.azuredatabricks.net/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
# MAGIC * Lesson: <a href="$./02-ETL-Process-Overview">ETL Process Overview</a> 
# MAGIC * Lesson: <a href="$./05-Applying-Schemas-to-JSON-Data">Applying Schemas to JSON Data</a> 
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC A common source of data in ETL pipelines is <a href="https://kafka.apache.org/" target="_blank">Apache Kafka</a>, or the managed alternative
# MAGIC <a href="https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-about" target="_blank">Azure Event Hubs</a>.
# MAGIC A common data type in these use cases is newline-separated JSON.
# MAGIC 
# MAGIC For this exercise, Tweets were streamed from the <a href="https://developer.twitter.com/en/docs" target="_blank">Twitter firehose API</a> into such an aggregation server and,
# MAGIC from there, dumped into the distributed file system.
# MAGIC 
# MAGIC Use these four exercises to perform ETL on the data in this bucket:  
# MAGIC <br>
# MAGIC 1. Extracting and Exploring the Data
# MAGIC 2. Defining and Applying a Schema
# MAGIC 3. Creating the Tables
# MAGIC 4. Loading the Results

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Extracting and Exploring the Data
# MAGIC 
# MAGIC First, review the data. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Explore the Folder Structure
# MAGIC 
# MAGIC Explore the mount and review the directory structure. Use `%fs ls`.  The data is located in `/mnt/training/twitter/firehose/`

# COMMAND ----------

# MAGIC %fs ls "/mnt/training/twitter/firehose/"

# COMMAND ----------

# Alternate
display(dbutils.fs.ls("/mnt/training/twitter/firehose/2018/01/08/18/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Explore a Single File
# MAGIC 
# MAGIC > "Premature optimization is the root of all evil." -Sir Tony Hoare
# MAGIC 
# MAGIC There are a few gigabytes of Twitter data available in the directory. Hoare's law about premature optimization is applicable here.  Instead of building a schema for the entire data set and then trying it out, an iterative process is much less error prone and runs much faster. Start by working on a single file before you apply your proof of concept across the entire data set.

# COMMAND ----------

# MAGIC %md
# MAGIC Read a single file.  Start with `twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4`. Find this in `/mnt/training/twitter/firehose/2018/01/08/18/`.  Save the results to the variable `df`.

# COMMAND ----------

# MAGIC %fs head "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"

# COMMAND ----------

# TODO
filename = "twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
df = (spark.read
     .json("/mnt/training/twitter/firehose/2018/01/08/18/" + filename)
     )
display(df)

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = df.columns

dbTest("ET1-P-08-02-01", 1744, df.count())
dbTest("ET1-P-08-02-02", True, "id" in cols)
dbTest("ET1-P-08-02-03", True, "text" in cols)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Display the schema.

# COMMAND ----------

# TODO
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Count the records in the file. Save the result to `dfCount`.

# COMMAND ----------

# TODO
dfCount = df.count()

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET1-P-08-03-01", 1744, dfCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Defining and Applying a Schema
# MAGIC 
# MAGIC Applying schemas is especially helpful for data with many fields to sort through. With a complex dataset like this, define a schema **that captures only the relevant fields**.
# MAGIC 
# MAGIC Capture the hashtags and dates from the data to get a sense for Twitter trends. Use the same file as above.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Understanding the Data Model
# MAGIC 
# MAGIC In order to apply structure to semi-structured data, you first must understand the data model.  
# MAGIC 
# MAGIC There are two forms of data models to employ: a relational or non-relational model.<br><br>
# MAGIC * **Relational models** are within the domain of traditional databases. [Normalization](https://en.wikipedia.org/wiki/Database_normalization) is the primary goal of the data model. <br>
# MAGIC * **Non-relational data models** prefer scalability, performance, or flexibility over normalized data.
# MAGIC 
# MAGIC Use the following relational model to define a number of tables to join together on different columns, in order to reconstitute the original data. Regardless of the data model, the ETL principles are roughly the same.
# MAGIC 
# MAGIC Compare the following [Entity-Relationship Diagram](https://en.wikipedia.org/wiki/Entity%E2%80%93relationship_model) to the schema you printed out in the previous step to get a sense for how to populate the tables.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ER-diagram.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Create a Schema for the `Tweet` Table
# MAGIC 
# MAGIC Create a schema for the JSON data to extract just the information that is needed for the `Tweet` table, parsing each of the following fields in the data model:
# MAGIC 
# MAGIC | Field | Type|
# MAGIC |-------|-----|
# MAGIC | tweet_id | integer |
# MAGIC | user_id | integer |
# MAGIC | language | string |
# MAGIC | text | string |
# MAGIC | created_at | string* |
# MAGIC 
# MAGIC *Note: Start with `created_at` as a string. Turn this into a timestamp later.
# MAGIC 
# MAGIC Save the schema to `tweetSchema`, use it to create a DataFrame named `tweetDF`, and use the same file used in the exercise above: `"/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You might need to reexamine the data schema. <br>
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** [Import types from `pyspark.sql.types`](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=pyspark%20sql%20types#module-pyspark.sql.types).

# COMMAND ----------

# TODO
path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"

tweetSchema = # FILL_IN
tweetDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.functions import col

schema = tweetSchema.fieldNames()
schema.sort()
tweetCount = tweetDF.filter(col("id").isNotNull()).count()

dbTest("ET1-P-08-04-01", 'created_at', schema[0])
dbTest("ET1-P-08-04-02", 'id', schema[1])
dbTest("ET1-P-08-04-03", 1491, tweetCount)

assert schema[0] == 'created_at' and schema[1] == 'id'
assert tweetCount == 1491

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create a Schema for the Remaining Tables
# MAGIC 
# MAGIC Finish off the full schema, save it to `fullTweetSchema`, and use it to create the DataFrame `fullTweetDF`. Your schema should parse all the entities from the ER diagram above.  Remember, smart small, run your code, and then iterate.

# COMMAND ----------

# TODO
path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
fullTweetSchema = # FILL_IN
fullTweetDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.functions import col

schema = fullTweetSchema.fieldNames()
schema.sort()
tweetCount = fullTweetDF.filter(col("id").isNotNull()).count()

assert tweetCount == 1491

dbTest("ET1-P-08-05-01", "created_at", schema[0])
dbTest("ET1-P-08-05-02", "entities", schema[1])
dbTest("ET1-P-08-05-03", 1491, tweetCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Creating the Tables
# MAGIC 
# MAGIC Apply the schema you defined to create tables that match the relational data model.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Filtering Nulls
# MAGIC 
# MAGIC The Twitter data contains both deletions and tweets.  This is why some records appear as null values. Create a DataFrame called `fullTweetFilteredDF` that filters out the null values.

# COMMAND ----------

# TODO
fullTweetFilteredDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET1-P-08-06-01", 1491, fullTweetFilteredDF.count())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Creating the `Tweet` Table
# MAGIC 
# MAGIC Twitter uses a non-standard timestamp format that Spark doesn't recognize. Currently the `created_at` column is formatted as a string. Create the `Tweet` table and save it as `tweetDF`. Parse the timestamp column using `unix_timestamp`, and cast the result as `TimestampType`. The timestamp format is `EEE MMM dd HH:mm:ss ZZZZZ yyyy`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use `alias` to alias the name of your columns to the final name you want for them.  
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** `id` corresponds to `tweet_id` and `user.id` corresponds to `user_id`.

# COMMAND ----------

# TODO
timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
tweetDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.types import TimestampType
t = tweetDF.select("createdAt").schema[0]

dbTest("ET1-P-08-07-01", TimestampType(), t.dataType)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Creating the Account Table
# MAGIC 
# MAGIC Save the account table as `accountDF`.

# COMMAND ----------

# TODO
accountDF = # <<FILL_IN>>

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = accountDF.columns

dbTest("ET1-P-08-08-01", True, "friendsCount" in cols)
dbTest("ET1-P-08-08-02", True, "screenName" in cols)
dbTest("ET1-P-08-08-03", 1491, accountDF.count())


print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 4: Creating Hashtag and URL Tables Using `explode`
# MAGIC 
# MAGIC Each tweet in the data set contains zero, one, or many URLs and hashtags. Parse these using the `explode` function so that each URL or hashtag has its own row.
# MAGIC 
# MAGIC In this example, `explode` gives one row from the original column `hashtags` for each value in an array. All other columns are left untouched.
# MAGIC 
# MAGIC ```
# MAGIC +---------------+--------------------+----------------+
# MAGIC |     screenName|            hashtags|explodedHashtags|
# MAGIC +---------------+--------------------+----------------+
# MAGIC |        zooeeen|[[Tea], [GoldenGl...|           [Tea]|
# MAGIC |        zooeeen|[[Tea], [GoldenGl...|  [GoldenGlobes]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|         [beats]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|           [90s]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|     [90shiphop]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|           [pac]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|        [legend]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|          [thug]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|         [music]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|     [westcoast]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|        [eminem]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|         [drdre]|
# MAGIC |mannydidthisone|[[beats], [90s], ...|          [trap]|
# MAGIC |  Satish0919995|[[BB11], [BiggBos...|          [BB11]|
# MAGIC |  Satish0919995|[[BB11], [BiggBos...|    [BiggBoss11]|
# MAGIC |  Satish0919995|[[BB11], [BiggBos...| [WeekendKaVaar]|
# MAGIC +---------------+--------------------+----------------+
# MAGIC ```
# MAGIC 
# MAGIC The concept of `explode` is similar to `pivot`.
# MAGIC 
# MAGIC Create the rest of the tables and save them to the following DataFrames:<br><br>
# MAGIC 
# MAGIC * `hashtagDF`
# MAGIC * `urlDF`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode" target="_blank">Find the documentation for `explode` here</a>

# COMMAND ----------

# TODO
hashtagDF = # FILL_IN
urlDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
hashtagCols = hashtagDF.columns
urlCols = urlDF.columns
hashtagDFCounts = hashtagDF.count()
urlDFCounts = urlDF.count()

dbTest("ET1-P-08-09-01", True, "hashtag" in hashtagCols)
dbTest("ET1-P-08-09-02", True, "displayURL" in urlCols)
dbTest("ET1-P-08-09-03", 394, hashtagDFCounts)
dbTest("ET1-P-08-09-04", 368, urlDFCounts)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 4: Loading the Results
# MAGIC 
# MAGIC Use DBFS as your target warehouse for your transformed data. Save the DataFrames in Parquet format to the following endpoints:  
# MAGIC 
# MAGIC | DataFrame    | Endpoint                                 |
# MAGIC |:-------------|:-----------------------------------------|
# MAGIC | `accountDF`  | `"/tmp/" + username + "/account.parquet"`|
# MAGIC | `tweetDF`    | `"/tmp/" + username + "/tweet.parquet"`  |
# MAGIC | `hashtagDF`  | `"/tmp/" + username + "/hashtag.parquet"`|
# MAGIC | `urlDF`      | `"/tmp/" + username + "/url.parquet"`    |
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you run out of storage in `/tmp`, use `.limit(10)` to limit the size of your DataFrames to 10 records.

# COMMAND ----------

# TODO
FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.dataframe import DataFrame

accountDF = spark.read.parquet("/tmp/" + username + "/account.parquet")
tweetDF = spark.read.parquet("/tmp/" + username + "/tweet.parquet")
hashtagDF = spark.read.parquet("/tmp/" + username + "/hashtag.parquet")
urlDF = spark.read.parquet("/tmp/" + username + "/url.parquet")

dbTest("ET1-P-08-10-01", DataFrame, type(accountDF))
dbTest("ET1-P-08-10-02", DataFrame, type(tweetDF))
dbTest("ET1-P-08-10-03", DataFrame, type(hashtagDF))
dbTest("ET1-P-08-10-04", DataFrame, type(urlDF))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT Next Steps
# MAGIC * Please complete the <a href="https://www.surveymonkey.com/r/WPD7YNV" target="_blank">short feedback survey</a>.  Your input is extremely important and shapes future course development.
# MAGIC * Congratulations, you have completed ETL Part 1!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>