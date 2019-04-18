# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploratory Analysis
# MAGIC 
# MAGIC Exploratory data analysis, or EDA, builds intuition from your data.  This lesson introduces the objectives and strategies for EDA including summary statistics, basic plotting, and correlations.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Identify the main objectives of exploratory analysis
# MAGIC * Calculate statistical moments to determine the center and spread of data
# MAGIC * Create plots of data including histograms and scatterplots
# MAGIC * Calculate correlations between variables
# MAGIC * Explore more advanced plots to visualize the relation between variables

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/baptj0tmzh?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/baptj0tmzh?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Exploratory Data Analysis
# MAGIC 
# MAGIC The goal of exploratory data analysis (EDA) is to build the intuition into a dataset that will inform how you model the data.  Models are only as strong as data that is fed into them and the more insight a data scientist has into their data, the stronger the features they'll create are and the more informed their design choices will be.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/eda.png" style="height: 500px; margin: 20px"/></div>
# MAGIC 
# MAGIC Exploratory analysis focuses on the following:<br><br>
# MAGIC 
# MAGIC 1. Gain basic intuition into the data
# MAGIC   - What does each feature represent?
# MAGIC   - Are your features categorical or continuous?
# MAGIC   - What data needs to be encoded? (e.g. mapping a variable for gender to a number)
# MAGIC   - What data types are you working with? (e.g. integers, strings)
# MAGIC 2. How is the data distributed?
# MAGIC   - What is the mean, median, and/or mode of each variable?
# MAGIC   - What is the variance, or spread, of each variable?
# MAGIC   - Are there outliers?  Missing values?
# MAGIC 3. How can we visualize the data?
# MAGIC   - Histograms, scatterplots, and boxplots
# MAGIC   - Correlation plots
# MAGIC 4. What hypotheses will I test with my models?
# MAGIC   - What features are correlated?
# MAGIC   - What are our ideal features and how can we build them if they're not already available?

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Count, Mean, and Standard Deviation
# MAGIC 
# MAGIC One way to start the EDA process is to calculate **n**, or the number of observations in the dataset, as well as the center and spread of the data.  We can break this down in the following way:<br><br>
# MAGIC 
# MAGIC 1. Count gives us the number of observed values, indicating the size of the dataset and whether there are missing values.
# MAGIC 2. Mean gives us the center of the data.  This could also be accomplished with the median or mode, depending on the data.
# MAGIC 3. Standard deviation quantifies how spread out the data is from the mean.  A small standard deviation indicates that the data is closely centered on the mean.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/standard-deviation.png" style="height: 250px; margin: 20px"/></div>
# MAGIC 
# MAGIC This chart indicates how to interpret standard deviation.  68% of the data is within one standard deviation (represented by `σ`) from the mean.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the <a href="https://en.wikipedia.org/wiki/Standard_deviation" target="_blank">Wikipedia article on Standard Deviation for more details on standard deviation.</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Boston dataset and call the `.describe()` method on it.

# COMMAND ----------

bostonDF = (spark.read
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv")
  .drop("_c0")
)

display(bostonDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC This shows that there are 506 observations in our dataset with no missing values since the counts of our records are all equal.  Take a look at just the output variable `medv`.

# COMMAND ----------

display(bostonDF.select("medv").describe())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC As seen with the mean and standard deviation, the average house price is $22,532 with 68% of the observations within $9,197 of that average.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `medv` is in 1000's of dollars

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Plotting, Distributions, and Outliers
# MAGIC 
# MAGIC Anscombe's Quartet is a set of four datasets that have identical means, standard deviations, and correlations.  Despite the fact that the descriptive statistics are the same, the underlying data is very different.  This is a cautionary tale for why data should always be plotted in the EDA phase.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/anscombes_quartet.svg" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC Plotting of a single variable is normally first done using a histogram.  This plots the general distribution of the data with the value on the x axis and the frequency the value appears on the y axis.  Plotting of two variables is normally first done with a scatterplot.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For plotting a single variable in a more more rigorous way, use <a href="https://en.wikipedia.org/wiki/Kernel_density_estimation" target="_blank">kernel density estimation</a> plots instead of histograms.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Plot a histogram of the median value in the housing dataset.  Display this column, click on the plotting option, and visualize it as a histogram.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/plotting.png" style="height: 300px; margin: 20px"/></div>

# COMMAND ----------

display(bostonDF.select("medv"))

# COMMAND ----------

# MAGIC %md
# MAGIC This plot shows the general distribution of median housing prices divided into 20 bins.  Now take a look at how the number of rooms compares to the median value.  Do this by selecting the scatterplot option from the plotting dropped menu.

# COMMAND ----------

display(bostonDF.select("rm", "medv"))

# COMMAND ----------

# MAGIC %md
# MAGIC By clicking the "LOESS" box under Plot Options, a local regression line is added showing the trend.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Databricks notebooks can display plots generated in pure Python:<br><br>
# MAGIC 
# MAGIC 1. Import `matplotlib`, `pandas`, and `seaborn`
# MAGIC 2. Create a `fig` object
# MAGIC 3. Use the `.toPandas()` DataFrame method to turn the Spark DataFrame into a Pandas DataFrame.  This way we can use Python's plotting libraries
# MAGIC 4. Use the `scatter_matrix` pandas function to plot a matrix of scatterplots
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Do not use `.toPandas()` on large datasets since a Pandas DataFrame must fit into the driver node of a cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC Plot a scatter matrix using the `pandas` Python library.

# COMMAND ----------

# MAGIC %python
# MAGIC import matplotlib.pyplot as plt
# MAGIC import pandas as pd
# MAGIC import seaborn as sns # importing for a better color scheme
# MAGIC 
# MAGIC try:
# MAGIC   bostonDF
# MAGIC except NameError: # Looks for local table if bostonDF not defined
# MAGIC   bostonDF = spark.table("boston")
# MAGIC 
# MAGIC fig, ax = plt.subplots()
# MAGIC pandasDF = bostonDF.select("rm", "crim", "lstat", "medv").toPandas()
# MAGIC 
# MAGIC pd.scatter_matrix(pandasDF)
# MAGIC 
# MAGIC display(fig.figure)

# COMMAND ----------

# MAGIC %md
# MAGIC The above plot shows histograms on the diagonal and scatterplots in the other quadrants.  The plot in the upper-left hand quarter is a histogram of the `rm` variable and the plot to its right is a scatterplot with `rm` as the y axis and `crim` as the x axis.  This shows us correlations across our variables.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Correlations
# MAGIC 
# MAGIC Correlation is the first way of understanding the relationship between two variables.  The correlation of two variables is the degree to which one variable increases linearly with the other:<br><br>
# MAGIC 
# MAGIC * A positive correlation of 1 means that for each unit increase in one variable, the same increase is seen in the other
# MAGIC * A negative correlation of -1 means that for each unit increase in one variable, the same decrease is seen in the other
# MAGIC * A correlation of 0 means that there is no association between the variables
# MAGIC 
# MAGIC More formally, correlation is computed as the following:<br><br>
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/correlation.svg" style="height: 60px; margin: 20px"/></div>
# MAGIC 
# MAGIC The correlation of variables X and Y is their covariance over the standard deviation of X multiplied by the standard deviation of Y.  The covariance is calculated by summing up the product of each value minus their respective mean (`μ`).

# COMMAND ----------

# MAGIC %md
# MAGIC To deepen our understanding of correlation, add columns to the `bostonDF` DataFrame that accomplishes the following:<br><br>
# MAGIC 
# MAGIC 1. `medvX3`: `medv` multiplied by 3
# MAGIC 3. `medvNeg`: `medv` multiplied by -3
# MAGIC 4. `random1`: a random number
# MAGIC 5. `random2`: a second random number
# MAGIC 6. `medvWithNoise`: `medv` multiplied by some random noise
# MAGIC 7. `medvWithNegativeNoise`: negative `medv` multiplied by some random noise

# COMMAND ----------

from pyspark.sql.functions import col, rand

bostonWithDummyDataDF = (bostonDF
  .select("medv")
  .withColumn("medvX3", col("medv")*3)
  .withColumn("medvNeg", col("medv")*-3)
  .withColumn("random1", rand(seed=41))
  .withColumn("random2", rand(seed=44))
  .withColumn("medvWithNoise", col("medv")*col("random1"))
  .withColumn("medvWithNegativeNoise", col("medv")*col("random1")*-1)
)

display(bostonWithDummyDataDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate correlations between all columns and `id`

# COMMAND ----------

for col in bostonWithDummyDataDF.columns:
  correlation = bostonWithDummyDataDF.stat.corr("medv", col)
  
  print("The correlation between columns 'id' and '{}': \t{}".format(col, correlation))

# COMMAND ----------

# MAGIC %md
# MAGIC The main takeaways:<br><br>
# MAGIC 
# MAGIC * The correlation between `id` and itself is 1, as is the correlation between `id` and `id` multiplied by 3.  
# MAGIC * The correlation between `id` and `id` multiplied by -3 is -1.  
# MAGIC * The correlations between `id` and random noise varied based on what was in the noise, but the results were closer to 0.  
# MAGIC * There were stronger positive and negative correlations in random noise multiplied by `id` than in random noise not correlated to `id`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Visualization Tools
# MAGIC 
# MAGIC There are a number of other helpful visualizations depending on the needs of your data.  These include:<br><br>
# MAGIC 
# MAGIC * <a href="https://en.wikipedia.org/wiki/Heat_map" target="_blank">Heat maps:</a> similar to a scattermatrix, heatmaps can be especially helpful at visualizing correlations between variables
# MAGIC * <a href="https://en.wikipedia.org/wiki/Box_plot" target="_blank">Box plots:</a> visualizes quantiles and outliers
# MAGIC * <a href="https://en.wikipedia.org/wiki/Q%E2%80%93Q_plot" target="_blank">Q-Q Plots:</a> visualizes two probability distributions
# MAGIC * <a href="https://en.wikipedia.org/wiki/Geographic_information_system" target="_blank">Maps and GIS:</a> visualizes geographically-bound data
# MAGIC * <a href="https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding" target="_blank">t-SNE:</a> plots high dimensional data (i.e. data that has many variables) by projecting it down into two-diminsional plot
# MAGIC * <a href="https://en.wikipedia.org/wiki/Time_series" target="_blank">Time series:</a> plots time-bound variables including run charts, lag plots, and wavelet spectrograms

# COMMAND ----------

# MAGIC %md
# MAGIC Assemble all of the `bostonDF` features into a single column `features`.  This allows us to use Spark's built-in correlation functionality.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.feature import VectorAssembler
# MAGIC 
# MAGIC assembler = VectorAssembler(inputCols=bostonDF.columns, outputCol="features")
# MAGIC 
# MAGIC bostonFeaturizedDF = assembler.transform(bostonDF)

# COMMAND ----------

display(bostonFeaturizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate the correlations across the entire dataset.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.stat import Correlation
# MAGIC 
# MAGIC pearsonCorr = Correlation.corr(bostonFeaturizedDF, 'features').collect()[0][0]
# MAGIC pandasDF = pd.DataFrame(pearsonCorr.toArray())
# MAGIC 
# MAGIC pandasDF.index, pandasDF.columns = bostonDF.columns, bostonDF.columns # Labels our index and columns so we can interpret the results

# COMMAND ----------

# MAGIC %md
# MAGIC Plot a heatmap of the correlations.  The redder the value, the stronger the positive correlation and the bluer the value the stronger the negative correlation.  Do this in pure Python rather than Spark because Python has better plotting functionality.

# COMMAND ----------

# MAGIC %python
# MAGIC import matplotlib.pyplot as plt
# MAGIC import seaborn as sns
# MAGIC 
# MAGIC fig, ax = plt.subplots()
# MAGIC sns.heatmap(pandasDF)
# MAGIC display(fig.figure)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: EDA on the Bike Sharing Dataset
# MAGIC 
# MAGIC Do exploratory analysis on the bike sharing dataset by calculating and interpreting summary statistics, creating basic plots, and calculating correlations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Summary Statistics
# MAGIC 
# MAGIC Calculate the count, mean, and standard deviation for each variable in the dataset.  What does each variable signify?  What is the spread of the data?

# COMMAND ----------

# MAGIC %md
# MAGIC Import the data.

# COMMAND ----------

bikeDF = (spark
  .read
  .option("header", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bikeSharing/data-001/hour.csv")
  .drop("instant", "dteday", "casual", "registered", "holiday", "weekday")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate count, mean, and standard deviation.

# COMMAND ----------

# TODO
display(bikeDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Plotting
# MAGIC 
# MAGIC Create the following plots:<br><br>
# MAGIC 
# MAGIC 1. A histogram of the dependent variable `cnt`
# MAGIC 2. A barplot of counts by hour
# MAGIC 3. A scattermatrix

# COMMAND ----------

# MAGIC %md
# MAGIC Create a histogram of the variable `cnt`.

# COMMAND ----------



# COMMAND ----------

# TODO
display(bikeDF.select("cnt"))

# COMMAND ----------

# MAGIC %md
# MAGIC Create a barplot of counts by hour.

# COMMAND ----------

import pyspark.sql.functions as func
# TODO
# display(bikeDF.groupBy("hr").agg(func.count("cnt")).alias("total").show())
display(bikeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a scattermatrix.  This can be done in Python or with the built-in Databricks functionality.

# COMMAND ----------

# TODO
display(bikeDF)

# COMMAND ----------

# MAGIC %python
# MAGIC import matplotlib.pyplot as plt
# MAGIC import pandas as pd
# MAGIC import seaborn as sns # importing for a better color scheme
# MAGIC 
# MAGIC 
# MAGIC fig, ax = plt.subplots()
# MAGIC pandasDF = bikeDF.toPandas()
# MAGIC 
# MAGIC pd.scatter_matrix(pandasDF)
# MAGIC 
# MAGIC display(fig.figure)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Correlations
# MAGIC 
# MAGIC Calculate the correlations of the different variables.  Start by using `VectorAssembler` to put all the variables into a single column `features`.

# COMMAND ----------

# TODO
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols = bikeDF.columns, outputCol = "features")
bikeFeatureizedDF = assembler.transform(bikeDF)

'''
assembler = VectorAssembler(inputCols=bostonDF.columns, outputCol="features")

bostonFeaturizedDF = assembler.transform(bostonDF)
'''

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate the correlation on the transformed `bikeDF`

# COMMAND ----------

# TODO
from pyspark.ml.stat import Correlation

pearsonCorr = Correlation.corr(bikeFeatureizedDF,"features").collect()[0][0]
pandasDF = pd.DataFrame(pearsonCorr.toArray())

pandasDF.index, pandasDF.columns = bikeDF.columns, bikeDF.columns # Labels our index and columns so we can interpret the results

display(pandasDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** Why is exploratory analysis important?  
# MAGIC **Answer:** The main goal of exploratory analysis is to build intuition from the dataset that will determine future modeling decisions.  This is often an overlooked step.  Having a limited understanding of the data leads to downstream problems like unexpected outputs and errors as well as under-performing models.
# MAGIC 
# MAGIC **Question:** What summary statistics are helpful?  
# MAGIC **Answer:** The most helpful statistics quantify the center and spread of the data (usually through the mean and standard deviation).  Counts (which reflect missing values) as well as the minimum and maximum are also informative.  A median can be used instead of a mean as well since a median is less influenced by outliers. 
# MAGIC 
# MAGIC **Question:** What are the most informative plots and how can I do that on Databricks?  
# MAGIC **Answer:** For a single variable, histograms and KDE plots are quite informative.  Box plots or violin plots are helpful as well.  Each shows the distribution of data and outliers in different ways.  When plotting multiple variables, scatterplots and bar charts can be helpful.  Scattermatrixes represent multiple features well.  This can be accomplished on Databricks either by using the built-in plotting functionality or by using the `display()` function to display plots generated with Python libraries like Matplotlib and Seaborn.
# MAGIC 
# MAGIC **Question:** What is correlation?  
# MAGIC **Answer:** Correlation looks at the linear relationship between two variables.  A positive correlation means that one variable increases linearly relative to another.  A negative correlation means that one variable increases as the other decreases.  No correlation means that the two variables are generally independent. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [ML Workflows]($./04-ML-Workflows ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on general mathematical functions in Spark?  
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html" target="_blank">Statistical and Mathematical Functions with DataFrames in Spark</a>
# MAGIC 
# MAGIC **Q:** Where can I find out more information on the machine learning Spark library and statistics?  
# MAGIC **A:** Check out <a href="https://spark.apache.org/docs/latest/ml-statistics.html" target="_blank">the Spark documentation</a>
# MAGIC 
# MAGIC **Q:** Can I do kernel density estimation in Spark?  
# MAGIC **A:** Yes.  See the <a href="https://spark.apache.org/docs/latest/mllib-statistics.html#kernel-density-estimation" target="_blank">Spark documentation for details</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>