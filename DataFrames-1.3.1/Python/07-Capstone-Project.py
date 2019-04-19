# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Project: Exploratory Data Analysis
# MAGIC Perform exploratory data analysis (EDA) to gain insights from a data lake.
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers and Data Scientists
# MAGIC * Secondary Audience: Data Analysts
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Lesson: <a href="$./04-Querying-Files">Querying Files with DataFrames</a>
# MAGIC * Lesson: <a href="$./05-Querying-Lakes">Querying Data Lakes with DataFrames</a>
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are a number of Parquet files containing 2016 crime data from seven United States cities:
# MAGIC 
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC 
# MAGIC 
# MAGIC The data is cleaned up a little, but has not been normalized. Each city reports crime data slightly differently, so
# MAGIC examine the data for each city to determine how to query it properly.
# MAGIC 
# MAGIC Your job is to use some of this data to gain insights about certain kinds of crimes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1
# MAGIC 
# MAGIC Start by creating DataFrames for Los Angeles, Philadelphia, and Dallas data.
# MAGIC 
# MAGIC Use `spark.read.parquet` to create named DataFrames for the files you choose. 
# MAGIC 
# MAGIC To read in the parquet file, use `crimeDataNewYorkDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")`
# MAGIC 
# MAGIC 
# MAGIC Use the following view names:
# MAGIC 
# MAGIC | City          | DataFrame Name            | Path to DBFS file
# MAGIC | ------------- | ------------------------- | -----------------
# MAGIC | Los Angeles   | `crimeDataLosAngelesDF`   | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet`
# MAGIC | Philadelphia  | `crimeDataPhiladelphiaDF` | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet`
# MAGIC | Dallas        | `crimeDataDallasDF`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet`

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/training/crime-data-2016/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# TODO

crimeDataLosAngelesDF = spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet")

# COMMAND ----------

# TEST - Run this cell to test your solution.

rowsLosAngeles  = crimeDataLosAngelesDF.count()
dbTest("DF-L7-crimeDataLA-count", rowsLosAngeles, 217945)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# TODO

crimeDataPhiladelphiaDF = spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet")

# COMMAND ----------

# TEST - Run this cell to test your solution.

rowsPhiladelphia  = crimeDataPhiladelphiaDF.count()
dbTest("DF-L7-crimeDataPA-count", rowsPhiladelphia, 168664)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

# TODO

crimeDataDallasDF = spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet")

# COMMAND ----------

# TEST - Run this cell to test your solution.

rowsDallas  = crimeDataDallasDF.count()
dbTest("DF-L7-crimeDataDAL-count", 99642, rowsDallas)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2
# MAGIC 
# MAGIC For each table, examine the data to figure out how to extract _robbery_ statistics.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Each city uses different values to indicate robbery. Commonly used terminology is "larceny", "burglary" or "robbery."  These challenges are common in Data Lakes.  These challenges are common in data lakes.  To simplify things, restrict yourself to only the word "robbery" (and not attempted-roberty, larceny, or burglary).
# MAGIC 
# MAGIC Explore the data for the three cities until you understand how each city records robbery information. If you don't want to worry about upper- or lower-case, 
# MAGIC remember to use the DataFrame `lower()` method to converts column values to lowercase.
# MAGIC 
# MAGIC Create a DataFrame containing only the robbery-related rows, as shown in the table below.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** For each table, focus your efforts on the column listed below.
# MAGIC 
# MAGIC Focus on the following columns for each table:
# MAGIC 
# MAGIC | DataFrame Name            | Robbery DataFrame Name  | Column
# MAGIC | ------------------------- | ----------------------- | -------------------------------
# MAGIC | `crimeDataLosAngelesDF`   | `robberyLosAngelesDF`   | `crimeCodeDescription`
# MAGIC | `crimeDataPhiladelphiaDF` | `robberyPhiladelphiaDF` | `ucr_general_description`
# MAGIC | `crimeDataDallasDF`       | `robberyDallasDF`       | `typeOfIncident`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# TODO
from pyspark.sql.functions import col, lower
robberyLosAngelesDF = (crimeDataLosAngelesDF
                      .filter(lower(col("crimeCodeDescription")) == "robbery"))

# COMMAND ----------

# TEST - Run this cell to test your solution.

totalLosAngeles  = robberyLosAngelesDF.count()
dbTest("DF-L7-robberyDataLA-count", 9048, totalLosAngeles)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# TODO

robberyPhiladelphiaDF  = (crimeDataPhiladelphiaDF
                         .filter(lower(col("ucr_general_description")) == "robbery"))

# COMMAND ----------

# TEST - Run this cell to test your solution.

totalPhiladelphia  = robberyPhiladelphiaDF.count()
dbTest("DF-L7-robberyDataPA-count", 6149, totalPhiladelphia)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

display(crimeDataDallasDF.filter(lower(col("typeOfIncident")).contains("robbery")).select("typeOfIncident").distinct())

# COMMAND ----------

# TODO

robberyDallasDF = (crimeDataDallasDF
                  .filter(lower(col("typeOfIncident")).startswith("robbery")))

# COMMAND ----------

# TEST - Run this cell to test your solution.

totalDallas = robberyDallasDF.count()
dbTest("DF-L7-robberyDataDAL-count", 6824, totalDallas)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3
# MAGIC 
# MAGIC Now that you have DataFrames of only the robberies in each city, create DataFrames for each city summarizing the number of robberies in each month.
# MAGIC 
# MAGIC Your DataFrames must contain two columns:
# MAGIC * `month`: The month number (e.g., 1 for January, 2 for February, etc.).
# MAGIC * `robberies`: The total number of robberies in the month.
# MAGIC 
# MAGIC Use the following DataFrame names and date columns:
# MAGIC 
# MAGIC 
# MAGIC | City          | DataFrame Name     | Date Column 
# MAGIC | ------------- | ------------- | -------------
# MAGIC | Los Angeles   | `robberiesByMonthLosAngelesDF` | `timeOccurred`
# MAGIC | Philadelphia  | `robberiesByMonthPhiladelphiaDF` | `dispatch_date_time`
# MAGIC | Dallas        | `robberiesByMonthDallasDF` | `startingDateTime`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For each city, figure out which column contains the date of the incident. Then, extract the month from that date.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# TODO

from pyspark.sql.functions import month, col, count
robberiesByMonthLosAngelesDF = (robberyLosAngelesDF
                               .select(month(col("timeOccurred")).alias("month"))
                               .groupby("month")
                               .agg(count("month").alias("robberies"))
                               .sort("month"))

display(robberiesByMonthLosAngelesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql import Row
la = list(robberiesByMonthLosAngelesDF.collect())

dbTest("DF-L7-robberyByMonthLA-counts", [Row(month=1, robberies=719), Row(month=2, robberies=675), Row(month=3, robberies=709), Row(month=4, robberies=713), Row(month=5, robberies=790), Row(month=6, robberies=698), Row(month=7, robberies=826), Row(month=8, robberies=765), Row(month=9, robberies=722), Row(month=10, robberies=814), Row(month=11, robberies=764), Row(month=12, robberies=853)], la)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# TODO

robberiesByMonthPhiladelphiaDF = (robberyPhiladelphiaDF
                               .select(month(col("dispatch_date_time")).alias("month"))
                               .groupby("month")
                               .agg(count("month").alias("robberies"))
                               .sort("month"))

# COMMAND ----------

# TEST - Run this cell to test your solution.
# convert to list so that we get deep compare (Array would be a shallow compare)
philadelphia  = list(robberiesByMonthPhiladelphiaDF.collect())

dbTest("DF-L7-robberyByMonthPA-counts", [Row(month=1, robberies=520), Row(month=2, robberies=416), Row(month=3, robberies=432), Row(month=4, robberies=466), Row(month=5, robberies=533), Row(month=6, robberies=509), Row(month=7, robberies=537), Row(month=8, robberies=561), Row(month=9, robberies=514), Row(month=10, robberies=572), Row(month=11, robberies=545), Row(month=12, robberies=544)], philadelphia )

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

# TODO

robberiesByMonthDallasDF = (robberyDallasDF
                               .select(month(col("startingDateTime")).alias("month"))
                               .groupby("month")
                               .agg(count("month").alias("robberies"))
                               .sort("month"))

# COMMAND ----------

# TEST - Run this cell to test your solution.

dallas  = list(robberiesByMonthDallasDF.collect())
dbTest("DF-L7-robberyByMonthDAL-counts", [Row(month=1, robberies=743), Row(month=2, robberies=435), Row(month=3, robberies=412), Row(month=4, robberies=594), Row(month=5, robberies=615), Row(month=6, robberies=495), Row(month=7, robberies=535), Row(month=8, robberies=627), Row(month=9, robberies=512), Row(month=10, robberies=603), Row(month=11, robberies=589), Row(month=12, robberies=664)], dallas)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Step 4
# MAGIC 
# MAGIC Plot the robberies per month for each of the three cities, producing a plot similar to the following:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/robberies-by-month.png" style="max-width: 700px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC 
# MAGIC When you first run the cell, you'll get an HTML table as the result. To configure the plot:
# MAGIC 
# MAGIC 1. Click the graph button.
# MAGIC 2. If the plot doesn't look correct, click the **Plot Options** button.
# MAGIC 3. Configure the plot similar to the following example.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-1.png" style="width: 440px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-2.png" style="width: 268px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-3.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# TODO

display(robberiesByMonthLosAngelesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# TODO

display(robberiesByMonthPhiladelphiaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

# TODO

display(robberiesByMonthDallasDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5
# MAGIC 
# MAGIC Create another DataFrame called `combinedRobberiesByMonthDF`, that combines all three robberies-per-month views into one.
# MAGIC In creating this view, add a new column called `city`, that identifies the city associated with each row.
# MAGIC The final view will have the following columns:
# MAGIC 
# MAGIC * `city`: The name of the city associated with the row. (Use the strings "Los Angeles", "Philadelphia", and "Dallas".)
# MAGIC * `month`: The month number associated with the row.
# MAGIC * `robbery`: The number of robbery in that month (for that city).
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You may want to apply the `union()` method in this example to combine the three datasets.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** It's easy to add new columns in DataFrames. For example, add a new column called `newColumn` to `originalDF` use `withColumn()` method as follows:
# MAGIC 
# MAGIC ```originalDF.withColumn("newColumn")``` 

# COMMAND ----------

from pyspark.sql.functions import lit

display(robberiesByMonthLosAngelesDF.withColumn("city",lit(str("LA"))))

# COMMAND ----------

# TODO

from pyspark.sql.functions import lit

combinedRobberiesByMonthDF = (robberiesByMonthLosAngelesDF.withColumn("city", lit(str("Los Angeles")))
                             .union(robberiesByMonthPhiladelphiaDF.withColumn("city", lit(str("Philadelphia"))))
                             .union(robberiesByMonthDallasDF.withColumn("city", lit(str("Dallas"))))
                             .select("city","month","robberies")
                             )

display(combinedRobberiesByMonthDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.

results = [ (r.city, r.month, r.robberies) for r in combinedRobberiesByMonthDF.collect() ]
expectedResults =  [ 
(u'Los Angeles', 1, 719), 
(u'Los Angeles', 2, 675), 
(u'Los Angeles', 3, 709), 
(u'Los Angeles', 4, 713),   
(u'Los Angeles', 5, 790), 
(u'Los Angeles', 6, 698), 
(u'Los Angeles', 7, 826), 
(u'Los Angeles', 8, 765), 
(u'Los Angeles', 9, 722), 
(u'Los Angeles', 10, 814), 
(u'Los Angeles', 11, 764), 
(u'Los Angeles', 12, 853), 
(u'Philadelphia', 1, 520), 
(u'Philadelphia', 2, 416), 
(u'Philadelphia', 3, 432), 
(u'Philadelphia', 4, 466),
(u'Philadelphia', 5, 533), 
(u'Philadelphia', 6, 509), 
(u'Philadelphia', 7, 537), 
(u'Philadelphia', 8, 561), 
(u'Philadelphia', 9, 514), 
(u'Philadelphia', 10, 572), 
(u'Philadelphia', 11, 545), 
(u'Philadelphia', 12, 544), 
(u'Dallas', 1, 743),
(u'Dallas', 2, 435), 
(u'Dallas', 3, 412), 
(u'Dallas', 4, 594), 
(u'Dallas', 5, 615), 
(u'Dallas', 6, 495), 
(u'Dallas', 7, 535), 
(u'Dallas', 8, 627), 
(u'Dallas', 9, 512), 
(u'Dallas', 10, 603), 
(u'Dallas', 11, 589), 
(u'Dallas', 12, 664)] 

dbTest("DF-L7-combinedRobberiesByMonth-counts", expectedResults, results)

print("Tests passed!")

# COMMAND ----------

combinedRobberiesByMonthDF.select("city")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 6
# MAGIC 
# MAGIC Graph the contents of `combinedRobberiesByMonthDF`, producing a graph similar to the following. (The diagram below deliberately
# MAGIC uses different data.)
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/combined-homicides.png" style="width: 800px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC 
# MAGIC Adjust the plot options to configure the plot properly, as shown below:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-4.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Order your results by `month`, then `city`.

# COMMAND ----------

# TODO

display(combinedRobberiesByMonthDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7
# MAGIC 
# MAGIC While the above graph is interesting, it's flawed: it's comparing the raw numbers of robberies, not the per capita robbery rates.
# MAGIC 
# MAGIC The DataFrame (already created) called `cityDataDF`  contains, among other data, estimated 2016 population values for all United States cities
# MAGIC with populations of at least 100,000. (The data is from [Wikipedia](https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population).)
# MAGIC 
# MAGIC * Use the population values in that table to normalize the robberies so they represent per-capita values (total robberies divided by population).
# MAGIC * Save your results in a DataFrame called `robberyRatesByCityDF`.
# MAGIC * The robbery rate value must be stored in a new column, `robberyRate`.
# MAGIC 
# MAGIC Next, graph the results, as above.

# COMMAND ----------

cityDataDF = spark.read.parquet("dbfs:/mnt/training/City-Data.parquet").withColumnRenamed("city", "cities")

display(cityDataDF)

# COMMAND ----------

# TODO

# rename "city" column to "cities" to avoid having two similarly named cols; also columns are case-blind
cityDataDF = spark.read.parquet("dbfs:/mnt/training/City-Data.parquet").withColumnRenamed("city", "cities")

robberyRatesByCityDF = (combinedRobberiesByMonthDF
                       .join(cityDataDF, combinedRobberiesByMonthDF.city == cityDataDF.cities)
                       .withColumn("robberyRate", lit(combinedRobberiesByMonthDF.robberies / cityDataDF.estPopulation2016))
                       .select("city","month","robberyRate")
                       .sort("city","month")
                       )

display(robberyRatesByCityDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
results = [ (r.city, r.month, '{:6f}'.format(r.robberyRate)) for r in robberyRatesByCityDF.orderBy("city", "month").collect() ]
expectedResults = [
  (u'Dallas',  1, '0.000564'),
  (u'Dallas',  2, '0.000330'),
  (u'Dallas',  3, '0.000313'),
  (u'Dallas',  4, '0.000451'),
  (u'Dallas',  5, '0.000467'),
  (u'Dallas',  6, '0.000376'),
  (u'Dallas',  7, '0.000406'),
  (u'Dallas',  8, '0.000476'),
  (u'Dallas',  9, '0.000388'),
  (u'Dallas', 10, '0.000458'),
  (u'Dallas', 11, '0.000447'),
  (u'Dallas', 12, '0.000504'),
  (u'Los Angeles',  1, '0.000181'),
  (u'Los Angeles',  2, '0.000170'),
  (u'Los Angeles',  3, '0.000178'),
  (u'Los Angeles',  4, '0.000179'),
  (u'Los Angeles',  5, '0.000199'),
  (u'Los Angeles',  6, '0.000176'),
  (u'Los Angeles',  7, '0.000208'),
  (u'Los Angeles',  8, '0.000192'),
  (u'Los Angeles',  9, '0.000182'),
  (u'Los Angeles', 10, '0.000205'),
  (u'Los Angeles', 11, '0.000192'),
  (u'Los Angeles', 12, '0.000215'),
  (u'Philadelphia',  1, '0.000332'),
  (u'Philadelphia',  2, '0.000265'),
  (u'Philadelphia',  3, '0.000276'),
  (u'Philadelphia',  4, '0.000297'),
  (u'Philadelphia',  5, '0.000340'),
  (u'Philadelphia',  6, '0.000325'),
  (u'Philadelphia',  7, '0.000343'),
  (u'Philadelphia',  8, '0.000358'),
  (u'Philadelphia',  9, '0.000328'),
  (u'Philadelphia', 10, '0.000365'),
  (u'Philadelphia', 11, '0.000348'),
  (u'Philadelphia', 12, '0.000347')]
dbTest("DF-L7-roberryRatesByCity-counts", expectedResults, results)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT Next Steps
# MAGIC * Please complete this short [feedback survey](https://www.surveymonkey.com/r/WH9L2LX).  Your input is extremely important and will shape future development.
# MAGIC * Congratulations, you have completed the Spark DataFrames course!

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC 
# MAGIC The crime data used in this notebook comes from the following locations:
# MAGIC 
# MAGIC | City          | Original Data 
# MAGIC | ------------- | -------------
# MAGIC | Boston        | <a href="https://data.boston.gov/group/public-safety" target="_blank">https&#58;//data.boston.gov/group/public-safety</a>
# MAGIC | Chicago       | <a href="https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2" target="_blank">https&#58;//data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2</a>
# MAGIC | Dallas        | <a href="https://www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data" target="_blank">https&#58;//www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data</a>
# MAGIC | Los Angeles   | <a href="https://data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq" target="_blank">https&#58;//data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq</a>
# MAGIC | New Orleans   | <a href="https://data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data" target="_blank">https&#58;//data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data</a>
# MAGIC | New York      | <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i" target="_blank">https&#58;//data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i</a>
# MAGIC | Philadelphia  | <a href="https://www.opendataphilly.org/dataset/crime-incidents" target="_blank">https&#58;//www.opendataphilly.org/dataset/crime-incidents</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>