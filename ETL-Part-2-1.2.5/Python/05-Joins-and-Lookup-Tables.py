# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins and Lookup Tables
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to join new records to existing tables in an ETL job.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Join new records to a pre-existing lookup table
# MAGIC * Employ table join best practices relevant to big data environments
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
# MAGIC src="//fast.wistia.net/embed/iframe/pvwi0sdvwu?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/pvwi0sdvwu?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Shuffle and Broadcast Joins
# MAGIC 
# MAGIC A common use case in ETL jobs involves joining new data to either lookup tables or historical data. You need different considerations to guide this process when working with distributed technologies such as Spark, rather than traditional databases that sit on a single machine.
# MAGIC 
# MAGIC Traditional databases join tables by pairing values on a given column. When all the data sits in a single database, it often goes unnoticed how computationally expensive row-wise comparisons are.  When data is distributed across a cluster, the expense of joins becomes even more apparent.
# MAGIC 
# MAGIC **A standard (or shuffle) join** moves all the data on the cluster for each table to a given node on the cluster. This is expensive not only because of the computation needed to perform row-wise comparisons, but also because data transfer across a network is often the biggest performance bottleneck of distributed systems.
# MAGIC 
# MAGIC By contrast, **a broadcast join** remedies this situation when one DataFrame is sufficiently small. A broadcast join duplicates the smaller of the two DataFrames on each node of the cluster, avoiding the cost of shuffling the bigger DataFrame.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/shuffle-and-broadcast-joins.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lookup Tables
# MAGIC 
# MAGIC Lookup tables are normally small, historical tables used to enrich new data passing through an ETL pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to mount the data.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Import a small table that will enrich new data coming into a pipeline.

# COMMAND ----------

labelsDF = spark.read.parquet("/mnt/training/day-of-week")

display(labelsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Import a larger DataFrame that gives a column to combine back to the lookup table. In this case, use Wikipedia site requests data.

# COMMAND ----------

from pyspark.sql.functions import col, date_format

pageviewsDF = (spark.read
  .parquet("/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/")
  .withColumn("dow", date_format(col("timestamp"), "u").alias("dow"))
)

display(pageviewsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Join the two DataFrames together.

# COMMAND ----------

pageviewsEnhancedDF = pageviewsDF.join(labelsDF, "dow")

display(pageviewsEnhancedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now aggregate the results to see trends by day of the week.
# MAGIC 
# MAGIC :NOTE: `pageviewsEnhancedDF` is a large DataFrame so it can take a while to process depending on the size of your cluster.

# COMMAND ----------

from pyspark.sql.functions import col

aggregatedDowDF = (pageviewsEnhancedDF
  .groupBy(col("dow"), col("longName"), col("abbreviated"), col("shortName"))  
  .sum("requests")                                             
  .withColumnRenamed("sum(requests)", "Requests")
  .orderBy(col("dow"))
)

display(aggregatedDowDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Exploring Broadcast Joins
# MAGIC 
# MAGIC In joining these two DataFrames together, no type of join was specified.  In order to examine this, look at the physical plan used to return the query. This can be done with the `.explain()` DataFrame method. Look for **BroadcastHashJoin** and/or **BroadcastExchange**.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/broadcasthashjoin.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

aggregatedDowDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC By default, Spark did a broadcast join rather than a shuffle join.  In other words, it broadcast `labelsDF` to the larger `pageviewsDF`, replicating the smaller DataFrame on each node of our cluster.  This avoided having to move the larger DataFrame across the cluster.
# MAGIC 
# MAGIC Take a look at the broadcast threshold by accessing the configuration settings.

# COMMAND ----------

threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print("Threshold: {0:,}".format( int(threshold) ))

# COMMAND ----------

# MAGIC %md
# MAGIC This is the maximize size in bytes for a table that broadcast to worker nodes.  Dropping it to `-1` disables broadcasting.

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %md
# MAGIC Now notice the lack of broadcast in the query physical plan.

# COMMAND ----------

pageviewsDF.join(labelsDF, "dow").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Next reset the original threshold.

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicitly Broadcasting Tables
# MAGIC 
# MAGIC There are two ways of telling Spark to explicitly broadcast tables. The first is to change the Spark configuration, which affects all operations. The second is to declare it using the `broadcast()` function in the `functions` package.

# COMMAND ----------

from pyspark.sql.functions import broadcast

pageviewsDF.join(broadcast(labelsDF), "dow").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Join a Lookup Table
# MAGIC 
# MAGIC Join a table that includes country name to a lookup table containing the full country name.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the Data
# MAGIC 
# MAGIC Create the following DataFrames:<br><br>
# MAGIC 
# MAGIC - `countryLookupDF`: A lookup table with ISO country codes located at `/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet`
# MAGIC - `logWithIPDF`: A server log including the results from an IPLookup table located at `/mnt/training/EDGAR-Log-20170329/enhanced/logDFwithIP.parquet`

# COMMAND ----------

# TODO
countryLookupDF = # FILL_IN
logWithIPDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-05-01-01", 249, countryLookupDF.count())
dbTest("ET2-P-05-01-02", 5000, logWithIPDF.count())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Broadcast the Lookup Table
# MAGIC 
# MAGIC Complete the following:<br><br>
# MAGIC 
# MAGIC - Create a new DataFrame `logWithIPEnhancedDF`
# MAGIC - Get the full country name by performing a broadcast join that broadcasts the lookup table to the server log
# MAGIC - Drop all columns other than `EnglishShortName`

# COMMAND ----------

# TODO
logWithIPEnhancedDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = set(logWithIPEnhancedDF.columns)

dbTest("ET2-P-05-02-01", True, "EnglishShortName" in cols and "ip" in cols)
dbTest("ET2-P-05-02-02", True, "alpha2Code" not in cols and "ISO31662SubdivisionCode" not in cols)
dbTest("ET2-P-05-02-03", 5000, logWithIPEnhancedDF.count())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** Why are joins expensive operations?  
# MAGIC **Answer:** Joins perform a large number of row-wise comparisons, making the cost associated with joining tables grow with the size of the data in the tables.
# MAGIC 
# MAGIC **Question:** What is the difference between a shuffle and broadcast join? How does Spark manage these differences?  
# MAGIC **Answer:** A shuffle join shuffles data between nodes in a cluster. By contrast, a broadcast join moves the smaller of two DataFrames to where the larger DataFrame sits, minimizing the overall data transfer. By default, Spark performs a broadcast join if the total number of records is below a certain threshold. The threshold can be manually specified or you can manually specify that a broadcast join should take place. Since the automatic determination of whether a shuffle join should take place is by number of records, this could mean that really wide data would take up significantly more space per record and should therefore be specified manually.
# MAGIC 
# MAGIC **Question:** What is a lookup table?  
# MAGIC **Answer:** A lookup table is small table often used for referencing commonly used data such as mapping cities to countries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Database Writes]($./06-Database-Writes ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I get more information on optimizing table joins where data skew is an issue?  
# MAGIC **A:** Check out the Databricks documentation on <a href="https://docs.databricks.com/spark/latest/spark-sql/skew-join.html" target="_blank">Skew Join Optimization</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>