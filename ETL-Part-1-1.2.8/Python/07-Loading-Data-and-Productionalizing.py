# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading Data and Productionalizing
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to productionalize code by scheduling notebooks for regular execution.
# MAGIC ## In this lesson you:
# MAGIC * Load data using the Apache Parquet format
# MAGIC * Automate a pipeline using the Databricks `Jobs` functionality
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
# MAGIC src="//fast.wistia.net/embed/iframe/b38tovvtgm?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/b38tovvtgm?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Introductory Productionalizing
# MAGIC 
# MAGIC Incorporating notebooks into production workflows will be covered in detail in later courses. This lesson focuses on two aspects of productionalizing: Parquet as a best practice for loading data from ETL jobs and scheduling jobs.
# MAGIC 
# MAGIC In the road map for ETL, this is the **Load and Automate** step:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-Process-4.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Writing Parquet
# MAGIC 
# MAGIC BLOB stores like S3 and the Azure Blob are the data storage option of choice on Databricks, and Parquet is the storage format of choice.  [Apache Parquet](https://parquet.apache.org/documentation/latest/) is a highly efficient, column-oriented data format that shows massive performance increases over other options such as CSV. For instance, Parquet compresses data repeated in a given column and preserves the schema from a write.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> When writing data to DBFS, the best practice is to use Parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/i7u61oyvcu?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/i7u61oyvcu?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to mount the data:

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Import Chicago crime data.

# COMMAND ----------

crimeDF = (spark.read
  .option("delimiter", "\t")
  .option("header", True)
  .option("timestampFormat", "mm/dd/yyyy hh:mm:ss a")
  .option("inferSchema", True)
  .csv("/mnt/training/Chicago-Crimes-2018.csv")
)
display(crimeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename the columns in `CrimeDF` so there are no spaces or invalid characters. This is required by Spark and is a best practice.  Use camel case.

# COMMAND ----------

cols = crimeDF.columns
titleCols = [''.join(j for j in i.title() if not j.isspace()) for i in cols]
camelCols = [column[0].lower()+column[1:] for column in titleCols]

crimeRenamedColsDF = crimeDF.toDF(*camelCols)
display(crimeRenamedColsDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Write to Parquet by calling the following method on a DataFrame: `.write.parquet("mnt/<destination>.parquet")`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Specify the write mode (for example, `overwrite` or `append`) using `.mode()`.
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Write to `/tmp/`, a directory backed by the Azure Blob or S3 available to all Databricks clusters. If your `/tmp/` directory is full, clear contents using `%fs rm -r /tmp/`.
# MAGIC 
# MAGIC [See the documentation for additional specifications.](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet)

# COMMAND ----------

crimeRenamedColsDF.write.mode("overwrite").parquet("/tmp/" + username + "/crime.parquet")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Review how this command writes the Parquet file. An advantage of Parquet is that, unlike a CSV file which is normally a single file, Parquet is distributed so each partition of data in the cluster writes to its own "part". Notice the different log data included in this directory.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Write other file formats in this same way (for example, `.write.csv("mnt/<destination>.csv")`)

# COMMAND ----------

display(dbutils.fs.ls("/tmp/" + username + "/crime.parquet"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Use the `repartition` DataFrame method to repartition the data to limit the number of separate parts.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> What appears to the user as a single DataFrame is actually data distributed across a cluster.  Each cluster holds _partitions_, or parts, of the data.  By repartitioning, we define how many different parts of our data to have.

# COMMAND ----------

crimeRenamedColsDF.repartition(1).write.mode("overwrite").parquet("/tmp/" + username + "/crimeRepartitioned.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Now look at how many parts are in the new folder. You have one part for each partition. Since you repartitioned the DataFrame with a value of `1`, now all the data is in `part-00000`.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("/tmp/" + username + "/crimeRepartitioned.parquet")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Automate by Scheduling a Job
# MAGIC 
# MAGIC Scheduling a job allows you to perform a batch process at a regular interval. Schedule email updates for successful completion and error logs.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Since jobs are not available in the Community Edition version of Databricks, you are unable to follow along in Community Edition.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC 1. Click **Jobs** in the left-hand panel of the screen.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/Jobs.png" style="height: 200px" style="margin-bottom: 20px; height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 2. Click **Create Job**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/Jobs2.png" style="height: 200px" style="margin-bottom: 20px; height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 3. Perform the following:
# MAGIC  - Name the job
# MAGIC  - Choose the notebook the job will execute
# MAGIC  - Specify the cluster
# MAGIC  - Choose a daily job
# MAGIC  - Send yourself an email alert
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/Jobs3.png" style="height: 200px" style="margin-bottom: 20px; height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to turn off the job so it does not execute indefinitely.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 1 (Optional): Productionalizing a Job
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Community Edition users are not able to complete this exercise.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1: Run All
# MAGIC 
# MAGIC Click **Run All** to confirm the notebook runs.  If there are any errors, fix them.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/Jobs4.png" style="height: 200px" style="margin-bottom: 20px; height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Schedule a Job
# MAGIC 
# MAGIC Schedule this notebook to run using the steps above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** What is the recommended storage format to use with Spark?
# MAGIC **Answer:** Apache Parquet is a highly optimized solution for data storage and is the recommended option for storage where possible.  In addition to offering benefits like compression, it's distributed, so a given partition of data writes to its own file, enabling parallel reads and writes. Formats like CSV are prone to corruption since a single missing comma could corrupt the data. Also, the data cannot be parallelized.
# MAGIC 
# MAGIC **Question:** How do you schedule a regularly occurring task in Databricks?
# MAGIC **Answer:** The Jobs tab of a Databricks notebook or the new [Jobs API](https://docs.databricks.com/api/latest/jobs.html) allows for job automation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Capstone Project]($./08-Capstone-Project ).
# MAGIC 
# MAGIC **At the end of this course, please complete the <a href="https://www.surveymonkey.com/r/WPD7YNV" target="_blank">short feedback survey</a>.  Your input is extremely important and shapes future course development.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I get more information on scheduling jobs on Databricks?
# MAGIC **A:** Check out the Databricks documentation on <a href="https://docs.databricks.com/user-guide/jobs.html" target="_blank">Scheduling Jobs on Databricks</a>
# MAGIC 
# MAGIC **Q:** How can I schedule complex jobs, such as those involving dependencies between jobs?
# MAGIC **A:** There are two options for complex jobs.  The easiest solution is <a href="https://docs.databricks.com/user-guide/notebooks/notebook-workflows.html" target="_blank">Notebook Workflows</a>, which involves using one notebook that triggers the execution of other notebooks. For more complexity, <a href="https://databricks.com/blog/2017/07/19/integrating-apache-airflow-with-databricks.html" target="_blank">Databricks integrates with the open source workflow scheduler Apache Airflow.</a>
# MAGIC 
# MAGIC **Q:** How do I perform spark-submit jobs?
# MAGIC **A:** Spark-submit is the process for running Spark jobs in the open source implementation of Spark.  [Jobs](https://docs.databricks.com/user-guide/jobs.html) and [the jobs API](https://docs.databricks.com/api/latest/jobs.html) are a robust option offered in the Databricks environment.  You can also launch spark-submit jobs through the jobs UI as well

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>