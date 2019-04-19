# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Querying Files with Dataframes
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to use DataFrames to query large data files.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Learn about Spark DataFrames.
# MAGIC * Query large files using Spark DataFrames.
# MAGIC * Visualize query results using charts.
# MAGIC 
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers and Data Scientists
# MAGIC * Secondary Audience: Data Analysts
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For this course all notebooks are provided, but knowing how to create notebooks is essential to your future work.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster; click  <b>Detached</b> in the upper left hand corner and then select your preferred cluster.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/g4ink372i8?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/g4ink372i8?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Introducing DataFrames
# MAGIC 
# MAGIC Under the covers, DataFrames are derived from data structures known as Resilient Distributed Datasets (RDDs). RDDs and DataFrames are immutable distributed collections of data. Let's take a closer look at what some of these terms mean before we understand how they relate to DataFrames:
# MAGIC 
# MAGIC * **Resilient**: They are fault tolerant, so if part of your operation fails, Spark  quickly recovers the lost computation.
# MAGIC * **Distributed**: RDDs are distributed across networked machines known as a cluster.
# MAGIC * **DataFrame**: A data structure where data is organized into named columns, like a table in a relational database, but with richer optimizations under the hood. 
# MAGIC 
# MAGIC Without the named columns and declared types provided by a schema, Spark wouldn't know how to optimize the executation of any computation. Since DataFrames have a schema, they use the Catalyst Optimizer to determine the optimal way to execute your code.
# MAGIC 
# MAGIC DataFrames were invented because the business community uses tables in a relational database, Pandas or R DataFrames, or Excel worksheets. A Spark DataFrame is conceptually equivalent to these, with richer optimizations under the hood and the benefit of being distributed across a cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Interacting with DataFrames
# MAGIC 
# MAGIC Once created (instantiated), a DataFrame object has methods attached to it. Methods are operations one can perform on DataFrames such as filtering,
# MAGIC counting, aggregating and many others.
# MAGIC 
# MAGIC > <b>Example</b>: To create (instantiate) a DataFrame, use this syntax: `df = ...`
# MAGIC 
# MAGIC To display the contents of the DataFrame, apply a `show` operation (method) on it using the syntax `df.show()`. 
# MAGIC 
# MAGIC The `.` indicates you are *applying a method on the object*.
# MAGIC 
# MAGIC In working with DataFrames, it is common to chain operations together, such as: `df.select().filter().orderBy()`.  
# MAGIC 
# MAGIC By chaining operations together, you don't need to save intermediate DataFrames into local variables (thereby avoiding the creation of extra objects).
# MAGIC 
# MAGIC Also note that you do not have to worry about how to order operations because the optimizier determines the optimal order of execution of the operations for you.
# MAGIC 
# MAGIC `df.select(...).orderBy(...).filter(...)`
# MAGIC 
# MAGIC versus
# MAGIC 
# MAGIC `df.filter(...).select(...).orderBy(...)`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### DataFrames and SQL
# MAGIC 
# MAGIC DataFrame syntax is more flexible than SQL syntax. Here we illustrate general usage patterns of SQL and DataFrames.
# MAGIC 
# MAGIC Suppose we have a data set we loaded as a table called `myTable` and an equivalent DataFrame, called `df`.
# MAGIC We have three fields/columns called `col_1` (numeric type), `col_2` (string type) and `col_3` (timestamp type)
# MAGIC Here are basic SQL operations and their DataFrame equivalents. 
# MAGIC 
# MAGIC Notice that columns in DataFrames are referenced by `col("<columnName>")`.
# MAGIC 
# MAGIC | SQL                                         | DataFrame (Python)                    |
# MAGIC | ------------------------------------------- | ------------------------------------- | 
# MAGIC | `SELECT col_1 FROM myTable`                 | `df.select(col("col_1"))`             | 
# MAGIC | `DESCRIBE myTable`                          | `df.printSchema()`                    | 
# MAGIC | `SELECT * FROM myTable WHERE col_1 > 0`     | `df.filter(col("col_1") > 0)`         | 
# MAGIC | `..GROUP BY col_2`                          | `..groupBy(col("col_2"))`             | 
# MAGIC | `..ORDER BY col_2`                          | `..orderBy(col("col_2"))`             | 
# MAGIC | `..WHERE year(col_3) > 1990`                | `..filter(year(col("col_3")) > 1990)` | 
# MAGIC | `SELECT * FROM myTable LIMIT 10`            | `df.limit(10)`                        |
# MAGIC | `display(myTable)` (text format)            | `df.show()`                           | 
# MAGIC | `display(myTable)` (html format)            | `display(df)`                         |
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can also run SQL queries with the special syntax `spark.sql("SELECT * FROM myTable")`
# MAGIC 
# MAGIC In this course you see many other usages of DataFrames. It is left up to you to figure out the SQL equivalents 
# MAGIC (left as exercises in some cases).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Data 
# MAGIC This lesson uses the `people-10m` data set, which is in Parquet format.
# MAGIC 
# MAGIC The data is fictitious; in particular, the Social Security numbers are fake.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/jghe5ncx5w?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/jghe5ncx5w?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the command below to see the contents of the `people-10m.parquet` file.

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-10m.parquet

# COMMAND ----------

peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the schema with the `printSchema` method. This tells you the field name, field type, and whether the column is nullable or not (default is true).

# COMMAND ----------

peopleDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Answer the following question:
# MAGIC > According to our data, which women were born after 1990?
# MAGIC 
# MAGIC Use the DataFrame `select` and `filter` methods.

# COMMAND ----------

from pyspark.sql.functions import year
display(
  peopleDF 
    .select("firstName","middleName","lastName","birthDate","gender") 
    .filter("gender = 'F'") 
    .filter(year("birthDate") > "1990")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Built-In Functions
# MAGIC 
# MAGIC Spark provides a number of <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in functions</a>, many of which can be used directly with DataFrames.  Use these functions in the `filter` expressions to filter data and in `select` expressions to create derived columns.
# MAGIC 
# MAGIC The following DataFrame statement finds women born after 1990; it uses the `year` function, and it creates a `birthYear` column on the fly.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/a0j4gkd45g?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/a0j4gkd45g?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

display(
  peopleDF.select("firstName","middleName","lastName",year("birthDate").alias('birthYear'),"salary") 
    .filter(year("birthDate") > "1990") 
    .filter("gender = 'F' ")
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Visualization
# MAGIC 
# MAGIC Databricks provides easy-to-use, built-in visualizations for your data. 
# MAGIC 
# MAGIC Display the data by invoking the Spark `display` function.
# MAGIC 
# MAGIC Visualize the query below by selecting the bar graph icon once the table is displayed:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/visualization-1.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/yoiex0ofyh?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/yoiex0ofyh?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC How many women were named Mary in each year?

# COMMAND ----------

marysDF = (peopleDF.select(year("birthDate").alias("birthYear")) 
  .filter("firstName = 'Mary' ") 
  .filter("gender = 'F' ") 
  .orderBy("birthYear") 
  .groupBy("birthYear") 
  .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC To start the visualization process, first apply the `display` function to the DataFrame. 
# MAGIC 
# MAGIC Next, click the graph button in the bottom left corner (second from left) to display data in different ways.
# MAGIC 
# MAGIC The data initially shows up in html format as an `n X 2` column where one column is the `birthYear` and another column is `count`.

# COMMAND ----------

display(marysDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare popularity of two names from 1990.

# COMMAND ----------

from pyspark.sql.functions import col
dordonDF = (peopleDF 
  .select(year("birthDate").alias("birthYear"), "firstName") 
  .filter((col("firstName") == 'Donna') | (col("firstName") == 'Dorothy')) 
  .filter("gender == 'F' ") 
  .filter(year("birthDate") > 1990) 
  .orderBy("birthYear") 
  .groupBy("birthYear", "firstName") 
  .count()
)

# COMMAND ----------

display(dordonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary Views
# MAGIC 
# MAGIC In DataFrames, <b>temporary views</b> are used to make the DataFrame available to SQL, and work with SQL syntax seamlessly.
# MAGIC 
# MAGIC A temporary view gives you a name to query from SQL, but unlike a table it exists only for the duration of your Spark Session. As a result, the temporary view will not carry over when you restart the cluster or switch to a new notebook. It also won't show up in the Data button on the menu on the left side of a Databricks notebook which provides easy access to databases and tables.
# MAGIC 
# MAGIC The statement in the following cells create a temporary view containing the same data.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/33o0hx71mz?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/33o0hx71mz?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

peopleDF.createOrReplaceTempView("People10M")

# COMMAND ----------

# MAGIC %md
# MAGIC To view the contents of temporary view, use select notation.

# COMMAND ----------

display(spark.sql("SELECT * FROM  People10M WHERE firstName = 'Donna' "))

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: In Databricks, `%sql` is the equivalent of `display()` combined with `spark.sql()`:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM People10M WHERE firstName = 'Donna' 

# COMMAND ----------

# MAGIC %md
# MAGIC Create a DataFrame with a more specific query.

# COMMAND ----------

womenBornAfter1990DF = (peopleDF 
  .select("firstName", "middleName", "lastName",year("birthDate").alias("birthYear"), "salary") 
  .filter(year("birthDate") > 1990) 
  .filter("gender = 'F' ") 
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Temporary Views from the `womenBornAfter1990DF` DataFrame

# COMMAND ----------

womenBornAfter1990DF.createOrReplaceTempView("womenBornAfter1990")

# COMMAND ----------

# MAGIC %md
# MAGIC Once a temporary view has been created, it can be queried as if it were a table. 
# MAGIC 
# MAGIC Find out how many Marys are in the WomenBornAfter1990 DataFrame.

# COMMAND ----------

display(spark.sql("SELECT count(*) FROM womenBornAfter1990 where firstName = 'Mary' "))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Create a DataFrame called top10FemaleFirstNamesDF that contains the 10 most common female first names out of the people data set.
# MAGIC 
# MAGIC * `firstName` - the first name
# MAGIC * `total` - the total number of rows with that first name
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** 
# MAGIC * You may need to break ties by using firstName because some of the totals are identical.
# MAGIC * To restrict the number of names to 10, you need to use the `limit(10)` method.
# MAGIC * You also need to use the `agg()` method to do a count of `firstName` and give it an alias.
# MAGIC * The `agg()` method is applied after the `groupBy` since it requires column values to be collected in some fashion.
# MAGIC * You will need to import the `count` and `desc` methods in Scala or Python, as appropriate.
# MAGIC 
# MAGIC Display the results.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC Create a DataFrame called `top10FemaleFirstNamesDF` and display the results.

# COMMAND ----------

# TODO
from pyspark.sql.functions import count, desc

# FILL IN
top10FemaleFirstNamesDF = (peopleDF 
  .select("firstName")
  .filter("gender = 'F' ")
  .groupby("firstName")
  .agg(count("firstName").alias('total'))
  .sort(desc("total"))                         
  .limit(10)       
)

# COMMAND ----------

top10FemaleFirstNamesDF.show()

# COMMAND ----------

top10FemaleNamesDF = top10FemaleFirstNamesDF.orderBy("firstName")

display(top10FemaleNamesDF)

# COMMAND ----------

from pyspark.sql import Row
results = top10FemaleNamesDF.collect()

dbTest("DF-L2-names-0", Row(firstName=u"Alesha",    total=1368), results[0])  
dbTest("DF-L2-names-1", Row(firstName=u"Alice",     total=1384), results[1])
dbTest("DF-L2-names-2", Row(firstName=u"Bridgette", total=1373), results[2])
dbTest("DF-L2-names-3", Row(firstName=u"Cristen",   total=1375), results[3])
dbTest("DF-L2-names-4", Row(firstName=u"Jacquelyn", total=1381), results[4])
dbTest("DF-L2-names-5", Row(firstName=u"Katherin",  total=1373), results[5])
dbTest("DF-L2-names-6", Row(firstName=u"Lashell",   total=1387), results[6])
dbTest("DF-L2-names-7", Row(firstName=u"Louie",     total=1382), results[7])
dbTest("DF-L2-names-8", Row(firstName=u"Lucille",   total=1384), results[8])
dbTest("DF-L2-names-9", Row(firstName=u"Sharyn",    total=1394), results[9]) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC 
# MAGIC Convert the DataFrame to a temporary view and display the contents of the temporary view.

# COMMAND ----------

# TODO
top10FemaleFirstNamesDF.createOrReplaceTempView("Top10FemaleFirstNames")
resultsDF = spark.sql("SELECT * FROM Top10FemaleFirstNames")
display(resultsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC * Spark DataFrames can be used to query Data Sets.
# MAGIC * Visualize the results of your queries with built-in Databricks graphs.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Review Questions
# MAGIC 
# MAGIC **Q:** What Scala keyword is used to *create* a DataFrame object?  
# MAGIC **A:** An object is created by invoking the `val` keyword, so `val myDataFrameDF`. 
# MAGIC 
# MAGIC **Q:** What methods (operations) can you perform on a DataFrame object?  
# MAGIC **A:** The full list is here: <a href="http://spark.apache.org/docs/2.0.0/api/python/pyspark.sql.html" target="_blank"> pyspark.sql module</a>
# MAGIC 
# MAGIC **Q:** Why do you chain methods (operations) `myDataFrameDF.select().filter().groupBy()`?  
# MAGIC **A:** To avoid the creation of temporary DataFrames as local variables. 
# MAGIC 
# MAGIC For example, you could have written the above as 
# MAGIC `val tempDF1 = myDataFrameDF.select()`,  `val tempDF2 = tempDF1.filter()` and
# MAGIC then `tempDF2.groupBy()`. 
# MAGIC 
# MAGIC This is syntatically equivalent, but, notice how you now have extra local variables.
# MAGIC 
# MAGIC **Q:** What is the DataFrame syntax to create a temporary view?    
# MAGIC **A:** ```myDF.createOrReplaceTempView("MyTempView")```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Aggregations, JOINs and Nested Queries]($./03-Joins-Aggregations ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>