# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Connecting to JDBC
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to connect to a number of data stores using JDBC.
# MAGIC ## In this lesson you:
# MAGIC * Read data from a JDBC connection 
# MAGIC * Parallelize your read operation to leverage distributed computation
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
# MAGIC src="//fast.wistia.net/embed/iframe/i07uvaoqgh?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/i07uvaoqgh?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Java Database Connectivity
# MAGIC 
# MAGIC Java Database Connectivity (JDBC) is an application programming interface (API) that defines database connections in Java environments.  Spark is written in Scala, which runs on the Java Virtual Machine (JVM).  This makes JDBC the preferred method for connecting to data whenever possible. Hadoop, Hive, and MySQL all run on Java and easily interface with Spark clusters.
# MAGIC 
# MAGIC Databases are advanced technologies that benefit from decades of research and development. To leverage the inherent efficiencies of database engines, Spark uses an optimization called predicate pushdown.  **Predicate pushdown uses the database itself to handle certain parts of a query (the predicates).**  In mathematics and functional programming, a predicate is anything that returns a Boolean.  In SQL terms, this often refers to the `WHERE` clause.  Since the database is filtering data before it arrives on the Spark cluster, there's less data transfer across the network and fewer records for Spark to process.  Spark's Catalyst Optimizer includes predicate pushdown communicated through the JDBC API, making JDBC an ideal data source for Spark workloads.
# MAGIC 
# MAGIC In the road map for ETL, this is the **Extract and Validate** step:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-Process-1.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recalling the Design Pattern
# MAGIC 
# MAGIC Recall the design pattern for connecting to data from the previous lesson:  
# MAGIC <br>
# MAGIC 1. Define the connection point.
# MAGIC 2. Define connection parameters such as access credentials.
# MAGIC 3. Add necessary options. 
# MAGIC 
# MAGIC After adhering to this, read data using `spark.read.options(<option key>, <option value>).<connection_type>(<endpoint>)`.  The JDBC connection uses this same formula with added complexity over what was covered in the lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/2clbjyxese?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/2clbjyxese?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to set up your environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Run the cell below to confirm you are using the right driver.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Each notebook has a default language that appears in upper corner of the screen next to the notebook name, and you can easily switch between languages in a notebook. To change languages, start your cell with `%python`, `%scala`, `%sql`, or `%r`.

# COMMAND ----------

# MAGIC %scala
# MAGIC // run this regardless of language type
# MAGIC Class.forName("org.postgresql.Driver")

# COMMAND ----------

# MAGIC %md
# MAGIC Define your database connection criteria. In this case, you need the hostname, port, and database name. 
# MAGIC 
# MAGIC Access the database `training` via port `5432` of a Postgres server sitting at the endpoint `server1.databricks.training`.
# MAGIC 
# MAGIC Combine the connection criteria into a URL.

# COMMAND ----------

jdbcHostname = "server1.databricks.training"
jdbcPort = 5432
jdbcDatabase = "training"

jdbcUrl = "jdbc:postgresql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a connection properties object with the username and password for the database.

# COMMAND ----------

connectionProps = {
  "user": "readonly",
  "password": "readonly"
}

# COMMAND ----------

# MAGIC %md
# MAGIC Read from the database by passing the URL, table name, and connection properties into `spark.read.jdbc()`.

# COMMAND ----------

accountDF = spark.read.jdbc(url=jdbcUrl, table="Account", properties=connectionProps)
display(accountDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Parallelizing JDBC Connections
# MAGIC 
# MAGIC The command above was executed as a serial read through a single connection to the database. This works well for small data sets; at scale, parallel reads are necessary for optimal performance.
# MAGIC 
# MAGIC See the [Managing Parallelism](https://docs.databricks.com/spark/latest/data-sources/sql-databases.html#managing-parallelism) section of the Databricks documentation.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1: Find the Range of Values in the Data
# MAGIC 
# MAGIC Parallel JDBC reads entail assigning a range of values for a given partition to read from. The first step of this divide-and-conquer approach is to find bounds of the data.
# MAGIC 
# MAGIC Calculate the range of values in the `insertID` column of `accountDF`. Save the minimum to `dfMin` and the maximum to `dfMax`.  **This should be the number itself rather than a DataFrame that contains the number.**  Use `.first()` to get a Scala or Python object.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the `min()` and `max()` functions in Python `pyspark.sql.functions` or Scala `org.apache.spark.sql.functions`.

# COMMAND ----------

from pyspark.sql.functions import min, max

# COMMAND ----------

# TODO
dfMin = accountDF.select(min("insertID")).first()[0]
dfMax = accountDF.select(max("insertID")).first()[0]

# COMMAND ----------

# TEST - Run this cell to test your solution

dbTest("ET1-P-04-01-01", 0, dfMin)
dbTest("ET1-P-04-01-02", 214748365087, dfMax)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Define the Connection Parameters.
# MAGIC 
# MAGIC <a href="https://docs.databricks.com/spark/latest/data-sources/sql-databases.html#manage-parallelism" target="_blank">Referencing the documentation,</a> define the connection parameters for this read.  Use 12 partitions.
# MAGIC 
# MAGIC Save the results to `accountDFParallel`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Setting the column for your parallel read introduces unexpected behavior due to a bug in Spark. To make sure Spark uses the capitalization of your column, use `'"insertID"'` for your column. <a href="https://github.com/apache/spark/pull/20370#issuecomment-359958843" target="_blank"> Monitor the issue here.</a>

# COMMAND ----------

help(spark.read.jdbc)

# COMMAND ----------

# TODO
accountDFParallel = (spark.read.jdbc(url=jdbcUrl,
    table="Account",
    column='"insertID"',
    lowerBound=dfMin,
    upperBound=dfMax,
    numPartitions=12,
    properties=connectionProps))

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET1-P-04-02-01", 12, accountDFParallel.rdd.getNumPartitions())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Compare the Serial and Parallel Reads
# MAGIC 
# MAGIC Compare the two reads with the `%timeit` function.  

# COMMAND ----------

# MAGIC %md
# MAGIC Display the number of partitions in each DataFrame by running the following:

# COMMAND ----------

print(accountDF.rdd.getNumPartitions())
print(accountDFParallel.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Invoke `%timeit` followed by calling a `.describe()`, which computes summary statistics, on both `accountDF` and `accountDFParallel`.

# COMMAND ----------

# MAGIC %timeit accountDF.describe()

# COMMAND ----------

# MAGIC %timeit accountDFParallel.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC What is the difference between serial and parallel reads?  Note that your results vary drastically depending on the cluster and number of partitions you use

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** What is JDBC?  
# MAGIC **Answer:** JDBC stands for Java Database Connectivity, and is a Java API for connecting to databases such as MySQL, Hive, and other data stores.
# MAGIC 
# MAGIC **Question:** How does Spark read from a JDBC connection by default?  
# MAGIC **Answer:** With a serial read.  With additional specifications, Spark conducts a faster, parallel read.  Parallel reads take full advantage of Spark's distributed architecture.
# MAGIC 
# MAGIC **Question:** What is the general design pattern for connecting to your data?  
# MAGIC **Answer:** The general design patter is as follows:
# MAGIC 0. Define the connection point
# MAGIC 0. Define connection parameters such as access credentials
# MAGIC 0. Add necessary options such as for headers or parallelization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Applying Schemas to JSON Data]($./05-Applying-Schemas-to-JSON-Data ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** My tool can't connect via JDBC.  Can I connect via <a href="https://en.wikipedia.org/wiki/Open_Database_Connectivity" target="_blank">ODBC instead</a>?  
# MAGIC **A:** Yes.  The best practice is generally to use JDBC connections wherever possible since Spark runs on the JVM.  In cases where JDBC is either not supported or is less performant, use the Simba ODBC driver instead.  See <a href="https://docs.databricks.com/user-guide/clusters/jdbc-odbc.html" target="_blank">the Databricks documentation on connecting BI tools</a> for more details.
# MAGIC 
# MAGIC 
# MAGIC **Q:** How can I connect my Spark cluster to Azure Cosmos DB?  
# MAGIC **A:** Microsoft has developed an Azure Cosmos DB Spark Connector. Start with the <a href="https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html#azure-cosmos-db" target="_blank">Databricks Azure Cosmos DB</a> documentation.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>