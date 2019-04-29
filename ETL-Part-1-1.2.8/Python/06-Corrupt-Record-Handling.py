# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Corrupt Record Handling
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; provide ways to handle corrupt records.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Define corruption logic to handle corrupt records
# MAGIC * Pipe corrupt records into a directory for later analysis
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Software Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Please use a <a href="https://docs.azuredatabricks.net/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
# MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/dataframes" target="_blank">DataFrames course from Databricks Academy</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/4m6z26iu8h?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/4m6z26iu8h?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Working with Corrupt Data
# MAGIC 
# MAGIC ETL pipelines need robust solutions to handle corrupt data. This is because data corruption scales as the size of data and complexity of the data application grow. Corrupt data includes:  
# MAGIC <br>
# MAGIC * Missing information
# MAGIC * Incomplete information
# MAGIC * Schema mismatch
# MAGIC * Differing formats or data types
# MAGIC * User errors when writing data producers
# MAGIC 
# MAGIC Since ETL pipelines are built to be automated, production-oriented solutions must ensure pipelines behave as expected. This means that **data engineers must both expect and systematically handle corrupt records.**
# MAGIC 
# MAGIC In the road map for ETL, this is the **Handle Corrupt Records** step:
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-Process-3.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/5y70n1k6vz?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/5y70n1k6vz?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to mount the data:

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Run the following cell, which contains a corrupt record, `{"a": 1, "b, "c":10}`:
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This is not the preferred way to make a DataFrame.  This code allows us to mimic a corrupt record you might see in production.

# COMMAND ----------

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .json(sc.parallelize(data))
)

display(corruptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC In the previous results, Spark parsed the corrupt record into its own column and processed the other records as expected. This is the default behavior for corrupt records, so you didn't technically need to use the two options `mode` and `columnNameOfCorruptRecord`.
# MAGIC 
# MAGIC There are three different options for handling corrupt records [set through the `ParseMode` option](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/ParseMode.scala#L34):
# MAGIC 
# MAGIC | `ParseMode` | Behavior |
# MAGIC |-------------|----------|
# MAGIC | `PERMISSIVE` | Includes corrupt records in a "_corrupt_record" column (by default) |
# MAGIC | `DROPMALFORMED` | Ignores all corrupted records |
# MAGIC | `FAILFAST` | Throws an exception when it meets corrupted records |
# MAGIC 
# MAGIC The following cell acts on the same data but drops corrupt records:

# COMMAND ----------

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("mode", "DROPMALFORMED")
  .json(sc.parallelize(data))
)
display(corruptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell throws an error once a corrupt record is found, rather than ignoring or saving the corrupt records:

# COMMAND ----------

try:
  data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

  corruptDF = (spark.read
    .option("mode", "FAILFAST")
    .json(sc.parallelize(data))
  )
  display(corruptDF)
  
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recommended Pattern: `badRecordsPath`
# MAGIC 
# MAGIC Databricks Runtime has [a built-in feature](https://docs.databricks.com/spark/latest/spark-sql/handling-bad-records.html) that saves corrupt records to a given end point. To use this, set the `badRecordsPath`.
# MAGIC 
# MAGIC This is a preferred design pattern since it persists the corrupt records for later analysis even after the cluster shuts down.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/4t8m5hbwp8?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/4t8m5hbwp8?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Before getting started, we need some place to store bad records.
# MAGIC 
# MAGIC We can use your `username` to create a unique `userhome` directory.
# MAGIC 
# MAGIC From there we can safely create temp files without any collision from other users.
# MAGIC 
# MAGIC :NOTE: We defined `username` and `userhome` for you in the call to `Classroom-Setup`

# COMMAND ----------

basePath = "{}/etl1p".format(userhome)
myBadRecords = "{}/badRecordsPath".format(basePath)

print("""Your temp directory is "{}" """.format(myBadRecords))

# COMMAND ----------

# MAGIC %md
# MAGIC And now let's put your `myBadRecords` to work:

# COMMAND ----------

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("badRecordsPath", myBadRecords)
  .json(sc.parallelize(data))
)
display(corruptDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC See the results in the path specified by `myBadRecords`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Recall that this directory is backed by the Azure Blob and is available to all clusters.

# COMMAND ----------

path = "{}/*/*/*".format(myBadRecords)
display(spark.read.json(path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Working with Corrupt Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Diagnose the Problem
# MAGIC 
# MAGIC Import the data used in the last lesson, which is located at `/mnt/training/UbiqLog4UCI/14_F/log*`.  Import the corrupt records in a new column `SMSCorrupt`.  <br>
# MAGIC 
# MAGIC Save only the columns `SMS` and `SMSCorrupt` to the new DataFrame `SMSCorruptDF`.

# COMMAND ----------

# TODO
from pyspark.sql.functions import col

SMSCorruptDF = (spark.read
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "SMSCorrupt")
                .json("/mnt/training/UbiqLog4UCI/14_F/log*")
                .select("SMS","SMSCorrupt")
                .filter(col("SMSCorrupt").isNotNull())
               )
display(SMSCorruptDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = set(SMSCorruptDF.columns)
SMSCount = SMSCorruptDF.cache().count()

dbTest("ET1-P-06-01-01", True, "SMS" in cols)
dbTest("ET1-P-06-01-02", True, "SMSCorrupt" in cols)
dbTest("ET1-P-06-01-03", 8, SMSCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Examine the corrupt records to determine what the problem is with the bad records.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Take a look at the name in metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC The entry `{"name": "mr Khojasteh"flash""}` should have single quotes around `flash` since the double quotes are interpreted as the end of the value.  It should read `{"name": "mr Khojasteh'flash'"}` instead.
# MAGIC 
# MAGIC The optimal solution is to fix the initial producer of the data to correct the problem at its source.  In the meantime, you could write ad hoc logic to turn this into a readable field.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Use `badRecordsPath`
# MAGIC 
# MAGIC Use the `badRecordsPath` option to save corrupt records to the directory specified by the `corruptPath` variable below.

# COMMAND ----------

# TODO

corruptPath = "{}/corruptSMS".format(basePath)

SMSCorruptDF2 = (spark.read
             .option("badRecordsPath", corruptPath)
             .json("/mnt/training/UbiqLog4UCI/14_F/log*")
            )
display(SMSCorruptDF2)

# COMMAND ----------

# TEST - Run this cell to test your solution
SMSCorruptDF2.count()

testPath = "{}/corruptSMS/*/*/*".format(basePath)
corruptCount = spark.read.json(testPath).count()

dbTest("ET1-P-06-02-01", True, corruptCount >= 8)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC One last step... let's clean up our temp files:

# COMMAND ----------

dbutils.fs.rm(basePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** By default, how are corrupt records dealt with using `spark.read.json()`?  
# MAGIC **Answer:** They appear in a column called `_corrupt_record`.
# MAGIC 
# MAGIC **Question:** How can a query persist corrupt records in separate destination?  
# MAGIC **Answer:** The Databricks feature `badRecordsPath` allows a query to save corrupt records to a given end point for the pipeline engineer to investigate corruption issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Loading Data and Productionalizing]($./07-Loading-Data-and-Productionalizing).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I get more information on dealing with corrupt records?  
# MAGIC **A:** Check out the Spark Summit talk on <a href="https://databricks.com/session/exceptions-are-the-norm-dealing-with-bad-actors-in-etl" target="_blank">Exceptions are the Norm: Dealing with Bad Actors in ETL</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>