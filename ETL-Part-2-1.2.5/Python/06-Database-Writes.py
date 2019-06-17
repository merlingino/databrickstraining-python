# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Database Writes
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to write to a number of target databases in parallel, storing the transformed data from from your ETL job.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Write to a target database in serial and in parallel
# MAGIC * Repartition DataFrames to optimize table inserts
# MAGIC * Coalesce DataFrames to minimize data shuffling
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Please use a <a href="https://docs.azuredatabricks.net/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
# MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction" target="_blank">ETL Part 1 course from Databricks Academy</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/qfso3zt2yg?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/qfso3zt2yg?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Database Writes in Spark
# MAGIC 
# MAGIC Writing to a database in Spark differs from other tools largely due to its distributed nature. There are a number of variables that can be tweaked to optimize performance, largely relating to how data is organized on the cluster. Partitions are the first step in understanding performant database connections.
# MAGIC 
# MAGIC **A partition is a portion of your total data set,** which is divided into many of these portions so Spark can distribute your work across a cluster. 
# MAGIC 
# MAGIC The other concept needed to understand Spark's computation is a slot (also known as a core). **A slot/core is a resource available for the execution of computation in parallel.** In brief, a partition refers to the distribution of data while a slot refers to the distribution of computation.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/partitions-and-cores.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC As a general rule of thumb, the number of partitions should be a multiple of the number of cores. For instance, with 5 partitions and 8 slots, 3 of the slots will be underutilized. With 9 partitions and 8 slots, a job will take twice as long as it waits for the extra partition to finish.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For a refresher on connecting to databases with JDBC, see Lesson 4 of <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction" target="_blank">ETL Part 1 course from Databricks Academy</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managing Partitions
# MAGIC 
# MAGIC In the context of JDBC database writes, **the number of partitions determine the number of connections used to push data through the JDBC API.** There are two ways to control this parallelism:  
# MAGIC 
# MAGIC | Function | Transformation Type | Use | Evenly distributes data across partitions? |
# MAGIC | :----------------|:----------------|:----------------|:----------------| 
# MAGIC | `.coalesce(n)`   | narrow (does not shuffle data) | reduce the number of partitions | no |
# MAGIC | `.repartition(n)`| wide (includes a shuffle operation) | increase the number of partitions | yes |

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to mount the data.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Start by importing a DataFrame of Wikipedia pageviews.

# COMMAND ----------

wikiDF = (spark.read
  .parquet("/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet")
)
display(wikiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC View the number of partitions by changing the DataFrame into an RDD and using the `.getNumPartitions()` method.  
# MAGIC Since the Parquet file was saved with 5 partitions, those partitions are retained when you import the data.

# COMMAND ----------

partitions = wikiDF.rdd.getNumPartitions()
print("Partitions: {0:,}".format( partitions ))

# COMMAND ----------

# MAGIC %md
# MAGIC To increase the number of partitions to 16, use `.repartition()`.

# COMMAND ----------

repartitionedWikiDF = wikiDF.repartition(16)
print("Partitions: {0:,}".format( repartitionedWikiDF.rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md
# MAGIC To reduce the number of partitions, use `.coalesce()`.

# COMMAND ----------

coalescedWikiDF = repartitionedWikiDF.coalesce(2)
print("Partitions: {0:,}".format( coalescedWikiDF.rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Configure Default Partitions
# MAGIC 
# MAGIC Spark uses a default value of 200 partitions, which comes from real-world experience by Spark engineers. This is an adjustable configuration setting. Run the following cell to see this value.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Get and set any number of different configuration settings in this manner. <a href="https://spark.apache.org/docs/latest/configuration.html" target="_blank">See the Spark documents</a> for details.

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Adjust the number of partitions with the following cell.  **This changes the number of partitions after a shuffle operation.**

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Now check to see how this changes an operation involving a data shuffle, such as an `.orderBy()`.  Recall that coalesced `coalescedWikiDF` to 2 partitions.

# COMMAND ----------

orderByPartitions = coalescedWikiDF.orderBy("requests").rdd.getNumPartitions()
print("Partitions: {0:,}".format( orderByPartitions ))

# COMMAND ----------

# MAGIC %md
# MAGIC The `.orderBy()` triggered the repartition of the DataFrame into 8 partitions.  Now reset the default value.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parallel Database Writes
# MAGIC 
# MAGIC Database writes are the inverse of what was covered in Lesson 4 of ETL Part 1.  In that lesson you defined the number of partitions in the call to the database.  
# MAGIC 
# MAGIC **By contrast and when writing to a database, the number of active connections to the database is determined by the number of partitions of the DataFrame.**

# COMMAND ----------

# MAGIC %md
# MAGIC Examine this by writing `wikiDF` to the `/tmp` directory.  Recall that `wikiDF` has 5 partitions.

# COMMAND ----------

wikiDF.write.mode("OVERWRITE").parquet(userhome+"/wiki.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Examine the number of partitions in `wiki.parquet`.

# COMMAND ----------

# MAGIC %python
# MAGIC for i in dbutils.fs.ls(userhome+"/wiki.parquet"):
# MAGIC   print(i)

# COMMAND ----------

# MAGIC %md
# MAGIC This file has 5 parts, meaning Spark wrote the data through 5 different connections to this directory in the file system.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examining in the Spark UI
# MAGIC 
# MAGIC Click the arrow next to `Spark Jobs` under the following code cell in order to see a breakdown of the job you triggered. Click the next arrow to see a breakdown of the stages.

# COMMAND ----------

wikiDF.repartition(12).write.mode("OVERWRITE").parquet(userhome+"/wiki.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 5 stages were initially triggered, one for each partition of our data.  When you repartitioned the DataFrame to 12 partitions, 12 stages were needed, one to write each partition of the data.  Run the following and observe how the repartitioning changes the number of stages.

# COMMAND ----------

wikiDF.repartition(10).write.mode("OVERWRITE").parquet(userhome+"/wiki.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC For more details, click `View` to examine the more details about the operation in the Spark UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A Note on Upserts
# MAGIC 
# MAGIC Upserts insert a record into a database if it doesn't already exist, and updates the existing record if it does.  **Upserts are not supported in core Spark** due to the transactional nature of upserting and the immutable nature of Spark. You can only append or overwrite.  Databricks offers a data management system called Databricks Delta that does allow for upserts and other transactional functionality. [See the Databricks Delta docs for more information.](https://docs.azuredatabricks.net/delta/index.html#delta-guide)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Changing Partitions
# MAGIC 
# MAGIC Change the number of partitions to prepare the optimal database write.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import Helper Functions and Data
# MAGIC 
# MAGIC A function is defined for you to print out the number of records in each DataFrame.  Run the following cell to define that function.

# COMMAND ----------

def printRecordsPerPartition(df):
  '''
  Utility method to count & print the number of records in each partition
  '''
  print("Per-Partition Counts:")
  
  def countInPartition(iterator): 
    yield __builtin__.sum(1 for _ in iterator)
    
  results = (df.rdd                   # Convert to an RDD
    .mapPartitions(countInPartition)  # For each partition, count
    .collect()                        # Return the counts to the driver
  )

  for result in results: 
    print("* " + str(result))

# COMMAND ----------

# MAGIC %md
# MAGIC Import the data to sitting in `/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet` to `wikiDF`.

# COMMAND ----------

# TODO
wikiDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-06-01-01", 7200000, wikiDF.count())
dbTest("ET2-P-06-01-02", ['timestamp', 'site', 'requests'], wikiDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Print the count of records by partition using `printRecordsPerPartition()`.

# COMMAND ----------

printRecordsPerPartition(wikiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Repartition the Data
# MAGIC 
# MAGIC Define three new DataFrames:
# MAGIC 
# MAGIC * `wikiDF1Partition`: `wikiDF` with 1 partition
# MAGIC * `wikiDF16Partition`: `wikiDF` with 16 partitions
# MAGIC * `wikiDF128Partition`: `wikiDF` with 128 partitions

# COMMAND ----------

# TODO
wikiDF1Partition = # FILL_IN
wikiDF16Partition = # FILL_IN
wikiDF128Partition = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution

dbTest("ET2-P-06-02-01", 1, wikiDF1Partition.rdd.getNumPartitions())
dbTest("ET2-P-06-02-02", 16, wikiDF16Partition.rdd.getNumPartitions())
dbTest("ET2-P-06-02-03", 128, wikiDF128Partition.rdd.getNumPartitions())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Examine the Distribution of Records
# MAGIC 
# MAGIC Use `printRecordsPerPartition()` to examine the distribution of records across the partitions.

# COMMAND ----------

# TODO
printRecordsPerPartition( <FILL_IN> )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Coalesce `wikiDF16Partition` and Examine the Results
# MAGIC 
# MAGIC Coalesce `wikiDF16Partition` to `10` partitions, saving the result to `wikiDF16PartitionCoalesced`.

# COMMAND ----------

# TODO
wikiDF16PartitionCoalesced = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution

dbTest("ET2-P-06-03-01", 10, wikiDF16PartitionCoalesced.rdd.getNumPartitions())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Examine the new distribution of data using `printRecordsPerPartition`.  Is the distribution uniform?  Why or why not?

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** How do you determine the number of connections to the database you write to?  
# MAGIC **Answer:** Spark makes one connection for each partition in your data. Increasing the number of partitions increases the database connections.
# MAGIC 
# MAGIC **Question:** How do you increase and decrease the number of partitions in your data?  
# MAGIC **Answer:** `.repartitions(n)` increases the number of partitions in your data. It can also decrease the number of partitions, but since this is a wide operation it should be used sparingly.  `.coalesce(n)` decreases the number of partitions in your data. If you use `.coalesce(n)` with a number greater than the current partitions, this DataFrame method will have no effect.
# MAGIC 
# MAGIC **Question:** How can you change the default number of partitions?  
# MAGIC **Answer:** Changing the configuration parameter `spark.sql.shuffle.partitions` will alter the default number of partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Table Management]($./07-Table-Management ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find more information on reading the Spark UI?  
# MAGIC **A:** Check out the Databricks blog on <a href="https://databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html" target="_blank">Understanding your Spark Application through Visualization</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>