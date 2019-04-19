# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL Process Overview
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to create an end-to-end _extract, transform, load (ETL)_ pipeline.
# MAGIC ## In this lesson you:
# MAGIC * Create a basic end-to-end ETL pipeline
# MAGIC * Demonstrate the Spark approach to ETL pipelines
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Please use a <a href="https://docs.azuredatabricks.net/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
# MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/dataframes" target="_blank">DataFrames course from Databricks Academy</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/wll3tz0ukb?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/wll3tz0ukb?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The Spark Approach
# MAGIC 
# MAGIC Spark offers a compute engine and connectors to virtually any data source. By leveraging easily scaled infrastructure and accessing data where it lives, Spark addresses the core needs of a big data application.
# MAGIC 
# MAGIC These principles comprise the Spark approach to ETL, providing a unified and scalable approach to big data pipelines: <br><br>
# MAGIC 
# MAGIC 1. Databricks and Spark offer a **unified platform** 
# MAGIC  - Spark on Databricks combines ETL, stream processing, machine learning, and collaborative notebooks.
# MAGIC  - Data scientists, analysts, and engineers can write Spark code in Python, Scala, SQL, and R.
# MAGIC 2. Spark's unified platform is **scalable to petabytes of data and clusters of thousands of nodes**.  
# MAGIC  - The same code written on smaller data sets scales to large workloads, often with only small changes.
# MAGIC 2. Spark on Databricks decouples data storage from the compute and query engine.  
# MAGIC  - Spark's query engine **connects to any number of data sources** such as S3, Azure Blob Storage, Redshift, and Kafka.  
# MAGIC  - This **minimizes costs**; a dedicated cluster does not need to be maintained and the compute cluster is **easily updated to the latest version** of Spark.
# MAGIC  
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/Workload_Tools_2-01.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ### A Basic ETL Job
# MAGIC 
# MAGIC In this lesson you use web log files from the <a href="https://www.sec.gov/dera/data/edgar-log-file-data-set.html" target="_blank">US Securities and Exchange Commission website</a> to do a basic ETL for a day of server activity. You will extract the fields of interest and load them into persistent storage.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/ur5peplrh5?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/ur5peplrh5?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster. Click <b>Detached</b> in the upper left hand corner and then select your preferred cluster.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to mount the data. Details on how this works are covered in the next lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC The Databricks File System (DBFS) is an HDFS-like interface to bulk data stores like Amazon's S3 and Azure's Blob storage service.
# MAGIC 
# MAGIC Pass the path `/mnt/training/EDGAR-Log-20170329/EDGAR-Log-20170329.csv` into `spark.read.csv`to access data stored in DBFS. Use the header option to specify that the first line of the file is the header.

# COMMAND ----------

path = "/mnt/training/EDGAR-Log-20170329/EDGAR-Log-20170329.csv"

logDF = (spark
  .read
  .option("header", True)
  .csv(path)
  .sample(withReplacement=False, fraction=0.3, seed=3) # using a sample to reduce data size
)

display(logDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, review the server-side errors, which have error codes in the 500s.  

# COMMAND ----------

from pyspark.sql.functions import col

serverErrorDF = (logDF
  .filter((col("code") >= 500) & (col("code") < 600))
  .select("date", "time", "extention", "code")
)

display(serverErrorDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Validation
# MAGIC 
# MAGIC One aspect of ETL jobs is to validate that the data is what you expect.  This includes:<br><br>
# MAGIC * Approximately the expected number of records
# MAGIC * The expected fields are present
# MAGIC * No unexpected missing values

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/k3mf97q7nn?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/k3mf97q7nn?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Take a look at the server-side errors by hour to confirm the data meets your expectations. Visualize it by selecting the bar graph icon once the table is displayed. <br><br>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/visualization.png" style="height: 400px" style="margin-bottom: 20px; height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, hour, col

countsDF = (serverErrorDF
  .select(hour(from_utc_timestamp(col("time"), "GMT")).alias("hour"))
  .groupBy("hour")
  .count()
  .orderBy("hour")
)

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The distribution of errors by hour meets the expectations.  There is an uptick in errors around midnight, possibly due to server maintenance at this time.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Saving Back to DBFS
# MAGIC 
# MAGIC A common and highly effective design pattern in the Databricks and Spark ecosystem involves loading structured data back to DBFS as a parquet file. Learn more about [the scalable and optimized data storage format parquet here](http://parquet.apache.org/).
# MAGIC 
# MAGIC Save the parsed DataFrame back to DBFS as parquet using the `.write` method.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> All clusters have storage available to them in the `/tmp/` directory.  In the case of Community Edition clusters, this is a small, but helpful, amount of storage.  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you run out of storage, use the command `dbutils.fs.rm("/tmp/<my directory>", True)` to recursively remove all items from a directory.  Note that this is a permanent action.

# COMMAND ----------

(serverErrorDF
  .write
  .mode("overwrite") # overwrites a file if it already exists
  .parquet("/tmp/" + username + "/log20170329/serverErrorDF.parquet")
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Our ETL Pipeline
# MAGIC 
# MAGIC Here's what the ETL pipeline you just built looks like.  In the rest of this course you will work with more complex versions of this general pattern.
# MAGIC 
# MAGIC | Code | Stage |
# MAGIC |------|-------|
# MAGIC | `logDF = (spark`                                                                          | Extract |
# MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.read`                                                           | Extract |
# MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.option("header", True)`                                         | Extract |
# MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.csv(<source>)`                                                  | Extract |
# MAGIC | `)`                                                                                       | Extract |
# MAGIC | `serverErrorDF = (logDF`                                                                  | Transform |
# MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.filter((col("code") >= 500) & (col("code") < 600))`             | Transform |
# MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.select("date", "time", "extention", "code")`                    | Transform |
# MAGIC | `)`                                                                                       | Transform |
# MAGIC | `(serverErrorDF.write`                                                                 | Load |
# MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.parquet(<destination>))`                                      | Load |
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This is a distributed job, so it can easily scale to fit the demands of your data set.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/qu6fxg1f6a?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/qu6fxg1f6a?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Perform an ETL Job
# MAGIC 
# MAGIC Write a basic ETL script that captures the 20 most active website users and load the results to DBFS.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a DataFrame of Aggregate Statistics
# MAGIC 
# MAGIC Create a DataFrame `ipCountDF` that uses `logDF` to create a count of each time a given IP address appears in the logs, with the counts sorted in descending order.  The result should have two columns: `ip` and `count`.

# COMMAND ----------

# TODO
ipCountDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
ip1, count1 = ipCountDF.first()
cols = set(ipCountDF.columns)

dbTest("ET1-P-02-01-01", "213.152.28.bhe", ip1)
dbTest("ET1-P-02-01-02", True, count1 > 500000 and count1 < 550000)
dbTest("ET1-P-02-01-03", True, 'count' in cols)
dbTest("ET1-P-02-01-03", True, 'ip' in cols)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Save the Results
# MAGIC 
# MAGIC Use your temporary folder to save the results back to DBFS as `"/tmp/" + username + "/ipCount.parquet"`
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** If you run out of space, use `%fs rm -r /tmp/<my directory>` to recursively (and permanently) remove all items from a directory.

# COMMAND ----------

# TODO
writePath = "/tmp/" + username + "/ipCount.parquet"
# FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.functions import desc

writePath = "/tmp/" + username + "/ipCount.parquet"

ipCountDF2 = (spark
  .read
  .parquet(writePath)
  .orderBy(desc("count"))
)
ip1, count1 = ipCountDF2.first()
cols = ipCountDF2.columns

dbTest("ET1-P-02-02-01", "213.152.28.bhe", ip1)
dbTest("ET1-P-02-02-02", True, count1 > 500000 and count1 < 550000)
dbTest("ET1-P-02-02-03", True, "count" in cols)
dbTest("ET1-P-02-02-04", True, "ip" in cols)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Check the load worked by using `%fs ls <path>`.  Parquet divides your data into a number of files.  If successful, you see a `_SUCCESS` file as well as the data split across a number of parts.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("/tmp/" + username + "/ipCount.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What does ETL stand for and what are the stages of the process?  
# MAGIC **Answer:** ETL stands for `extract-transform-load`
# MAGIC 0. *Extract* refers to ingesting data.  Spark easily connects to data in a number of different sources.
# MAGIC 0. *Transform* refers to applying structure, parsing fields, cleaning data, and/or computing statistics.
# MAGIC 0. *Load* refers to loading data to its final destination, usually a database or data warehouse.
# MAGIC 
# MAGIC **Question:** How does the Spark approach to ETL deal with devops issues such as updating a software version?  
# MAGIC **Answer:** By decoupling storage and compute, updating your Spark version is as easy as spinning up a new cluster.  Your old code will easily connect to S3, the Azure Blob, or other storage.  This also avoids the challenge of keeping a cluster always running, such as with Hadoop clusters.
# MAGIC 
# MAGIC **Question:** How does the Spark approach to data applications differ from other solutions?  
# MAGIC **Answer:** Spark offers a unified solution to use cases that would otherwise need individual tools. For instance, Spark combines machine learning, ETL, stream processing, and a number of other solutions all with one technology.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Connecting to Azure Blob Storage]($./03-Connecting-to-Azure-Blob-Storage ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I get more information on building ETL pipelines?  
# MAGIC **A:** Check out the Spark Summit talk on <a href="https://databricks.com/session/building-robust-etl-pipelines-with-apache-spark" target="_blank">Building Robust ETL Pipelines with Apache Spark</a>
# MAGIC 
# MAGIC **Q:** Where can I find out more information on moving from traditional ETL pipelines towards Spark?  
# MAGIC **A:** Check out the Spark Summit talk <a href="https://databricks.com/session/get-rid-of-traditional-etl-move-to-spark" target="_blank">Get Rid of Traditional ETL, Move to Spark!</a>
# MAGIC 
# MAGIC **Q:** What are the visualization options in Databricks?  
# MAGIC **A:** Databricks provides a wide variety of <a href="https://docs.databricks.com/user-guide/visualizations/index.html" target="_blank">built-in visualizations</a>.  Databricks also supports a variety of 3rd party visualization libraries, including <a href="https://d3js.org/" target="_blank">d3.js</a>, <a href="https://matplotlib.org/" target="_blank">matplotlib</a>, <a href="http://ggplot.yhathq.com/" target="_blank">ggplot</a>, and <a href="https://plot.ly/" target="_blank">plotly<a/>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>