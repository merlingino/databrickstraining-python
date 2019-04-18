# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Classification
# MAGIC 
# MAGIC While linear regression models continuous variables, logistic regression classifies variables into two or more groups.  This lesson trains binomial and multi-class logistic regression models and examines evaluation metrics for classification tasks.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Identify classification versus regression tasks
# MAGIC * Train a binomial logistic regression model
# MAGIC * Use a confusion matrix to examine model performance
# MAGIC * Choose an evaluation metric specific to your use case
# MAGIC * Train a multi-class classification model

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/1juy91ri2b?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/1juy91ri2b?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The Logistic Regression Classifier
# MAGIC 
# MAGIC Linear regression models continuous variables where the result can be any value from negative infinity to infinity including decimals.  Many use cases involve modeling qualitative or categorical numbers instead, such as classifying a user on a website as fraudulent or not.
# MAGIC 
# MAGIC Classifiers fall into two main categories.  **Binomial, or binary, classification classifies data into two different categories,** such as whether or not a patient has a type of cancer.  **Multi-class classification classifies data into more than two categories,** such as predicting which category of object is in an image.
# MAGIC 
# MAGIC **Logistic regression models the _probability_ that a given set of features belong to a given category.**  Given that we're modeling a probability, results are bounded between 0 and 1.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/logistic-regression.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC The above plot visualizes the relationship between breast cells and one input variable: thickness of cells.  Since the model returns the probabilities of an observation being of a given class, we can decide which cut-off threshold to use to classify a sample as cancerous or not.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Binomial Logistic Regression
# MAGIC 
# MAGIC Binomial logistic regression classifies data between two categories, giving us the probability (`p(X)`) associated with the observed data being of a particular category.  Formally, it differs from linear regression as follows:
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;Linear Regression:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`Y ≈ β<sub>0</sub> + β<sub>1</sub>X`<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;Logistic Regression:&nbsp;&nbsp;&nbsp;`p(X) ≈ β<sub>0</sub> + β<sub>1</sub>X` 
# MAGIC 
# MAGIC Note that p(X) is in log odds.  This is explored further below.
# MAGIC 
# MAGIC Start by looking at the breast cancer dataset that contains information on breast cells and whether they are cancerous.

# COMMAND ----------

from pyspark.sql.functions import col

cols = ["index",
 "sample-code-number",
 "clump-thickness",
 "uniformity-of-cell-size",
 "uniformity-of-cell-shape",
 "marginal-adhesion",
 "single-epithelial-cell-size",
 "bare-nuclei",
 "bland-chromatin",
 "normal-nucleoli",
 "mitoses",
 "class"]

cancerDF = (spark.read  
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/cancer/biopsy/biopsy.csv")
)

cancerDF = (cancerDF    # Add column names and drop nulls
  .toDF(*cols)
  .withColumn("bare-nuclei", col("bare-nuclei").isNotNull().cast("integer"))
)

display(cancerDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Classification models are sensitive to how much data we have in each class.  View the class balance by grouping the data on our label.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In the case of unbalanced classes, one solution is to use to pass weights into the model allowing for a greater penalty for misclassifying the underrepresented class.

# COMMAND ----------

display(cancerDF.groupby("class").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Our classes are well balanced.  Turn the label into a numerical value and create a column `feature` for just `clump-thickness`.

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler

indexer = StringIndexer(inputCol="class", outputCol="is-malignant")
cancerIndexedDF = indexer.fit(cancerDF).transform(cancerDF)

assembler = VectorAssembler(inputCols=["clump-thickness"], outputCol="features")
cancerIndexedAssembledDF = assembler.transform(cancerIndexedDF)

display(cancerIndexedAssembledDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Train the logistic regression model using only the input feature `clump-thickness` and look at the resulting coefficient.

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

logr = LogisticRegression(featuresCol="features", labelCol="is-malignant")

logrModel = logr.fit(cancerIndexedAssembledDF)

print("β0 (intercept): {}".format(logrModel.intercept))
print("β1 (coefficient for rm): {}".format(logrModel.coefficients[0]))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Model Interpretation
# MAGIC 
# MAGIC In linear regression, increasing a given feature `X` by one unit increased our prediction by `X` multiplied by the corresponding coefficient.  In logistic regression, increasing a given feature `X` by one unit increases the log odds by the corresponding coefficient.  In other words, the odds that a clump size of 9 is cancerous is :
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`odds of cancer = ε<sup>β<sub>0</sub> + β<sub>1</sub>X</sup>`<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`odds of cancer = ε<sup>-5.2 + .94 x 9</sup>`<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`odds of cancer = ε<sup>3.26</sup>`<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`odds of cancer = 26`<br>
# MAGIC 
# MAGIC The odds of cancer with a clump size of 9 is therefore 26 to 1, or .96.

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at how the model makes predictions.  The `probability` column gives us the probability of being of class 0 and of being in class 1.

# COMMAND ----------

from pyspark.sql.functions import desc

transformedDF = logrModel.transform(cancerIndexedAssembledDF)

display(transformedDF
  .select("clump-thickness", "class", "is-malignant", "probability")
  .distinct()
  .orderBy(desc("clump-thickness"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate this value manually using the intercept and coefficient.

# COMMAND ----------

from math import exp

odds = exp(-5.2 + .94 * 9)     # Exponentiate the formula since logistic regression is log odds
probability = 1 - (1 / odds)   # Convert odds to probability

print("Prediction for odds of cancer: {} to 1".format( round(odds, 1) ))
print("Prediction for probability of cancer: {}".format(probability))

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at how the model predicted the probability.

# COMMAND ----------

(transformedDF
  .filter(col("clump-thickness") == 9)
  .select("probability")
  .first()[0]
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Model Performance
# MAGIC 
# MAGIC A confusion matrix is a table that reports the number of false positives, false negatives, true positives, and true negatives.  This is the basis of a number of different evaluation metrics such as accuracy, precision, and area under the ROC curve.  For instance, accuracy is calculated by adding the true positives and true negatives and dividing by the total number of records.  The best accuracy metric for a given use case depends on how we want to optimize between true and false predictions.
# MAGIC 
# MAGIC For the model trained above, the confusion matrix looks as follows:
# MAGIC 
# MAGIC | n = 699           | Predicted Positive   | Predicted Negative   |
# MAGIC |-------------------|----------------------|----------------------|
# MAGIC | Actually Positive | True Positives:  165 | False Positives:  21 |
# MAGIC | Actually Negative | False Negatives: 76  | True Negatives:  437 |
# MAGIC 
# MAGIC Various model evaluation metrics are based around this table.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://en.wikipedia.org/wiki/Confusion_matrix" target="_blank">the Wikipedia article on Confusion Matrix</a> for more information on various evaluation metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC Print out the confusion matrix.

# COMMAND ----------

n = transformedDF.count()
TP = transformedDF.filter((col("is-malignant") == 1) & (col("prediction") == 1)).count()
FP = transformedDF.filter((col("is-malignant") == 0) & (col("prediction") == 1)).count()
FN = transformedDF.filter((col("is-malignant") == 1) & (col("prediction") == 0)).count()
TN = transformedDF.filter((col("is-malignant") == 0) & (col("prediction") == 0)).count()

print("n = {}\t\t\tPredicted Positive \tPredicted Negative".format(n))
print("Actually Positive\t{} \t\t\t{}".format(TP, FP))
print("Actually Negative\t{} \t\t\t{}".format(FN, TN))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC One way of visualizing the trade-off between true positives and false positives is by plotting them on a ROC Curve.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://en.wikipedia.org/wiki/Receiver_operating_characteristic" target="_blank">the Wikipedia article on ROC Curves</a> for more information.

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

falsePositives, truePositives = [], []

for row in logrModel.summary.roc.collect():
  falsePositives.append(row.FPR)
  truePositives.append(row.TPR)

fig = plt.figure()

plt.plot(falsePositives, truePositives)
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("ROC Curve")

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC The associated evaluation metric takes the total area under this curve, which is bounded between 0 and 1.

# COMMAND ----------

logrModel.summary.areaUnderROC

# COMMAND ----------

# MAGIC %md
# MAGIC Retrain the model using all of the relevant features.  This excludes the index and sequence number, which would not provide relevant information to our model.

# COMMAND ----------

assembler = VectorAssembler(inputCols=cols[2:-1], outputCol="features")
cancerIndexedAssembledDF2 = assembler.transform(cancerIndexedDF)

logr = LogisticRegression(labelCol="is-malignant", featuresCol="features")

logrModel2 = logr.fit(cancerIndexedAssembledDF2)

logrModel2.coefficients

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at how the area under the ROC curve has improved.

# COMMAND ----------

logrModel2.summary.areaUnderROC

# COMMAND ----------

# MAGIC %md
# MAGIC As we can see, the model performs relatively well at predicting cancer based upon the available features.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-Class Classification
# MAGIC 
# MAGIC Classification between multiple classes using logistic regression is known as multi-class or **multinomial logistic regression.**  While the underlying mathematics are quite different, the Spark API is the same.  To predict between different classes, set the `family` argument for `LogisticRegression` to `multinomial`.  This model then provides probabilities associated with each class.
# MAGIC 
# MAGIC Continue on to the exercise where you'll train a multinomial model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Training Multinomial Logistic Regression
# MAGIC 
# MAGIC Train a multinomial logistic regression model using the Iris dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the Data
# MAGIC 
# MAGIC Import the data and clean the column names, which can cause issues for Spark.

# COMMAND ----------

irisDF = (spark.read
  .option("INFERSCHEMA", True)
  .option("HEADER", True)
  .csv("/mnt/training/iris/iris.csv")
  .drop("_c0")
)

cols = [col.replace(".", "-").lower() for col in irisDF.columns]
irisDF = irisDF.toDF(*cols)

display(irisDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Create a Pipeline
# MAGIC 
# MAGIC Create a pipeline of three stages:
# MAGIC 
# MAGIC 1. `indexer`: a `StringIndexer` that takes `species` as an input and outputs `speciesClass`
# MAGIC 2. `assembler`: a `VectorAssembler` that takes all the features in the dataset and outputs `features`
# MAGIC 3. `multinomialRegression`: a `LogisticRegression` that takes `features` and uses the `multinomial` family
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=vectorassembler#pyspark.ml.classification.LogisticRegression" target="_blank">LogisticRegression</a> documentation for more details.

# COMMAND ----------

from pyspark.ml import Pipeline

# TODO
indexer = StringIndexer(inputCol="species", outputCol="speciesClass")
assembler = VectorAssembler(inputCols=irisDF.columns[:-1], outputCol="features")
multinomialRegression = LogisticRegression(labelCol="speciesClass", featuresCol="features", family="multinomial")

pipeline = Pipeline(stages=[
  indexer, 
  assembler, 
  multinomialRegression
])

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler

dbTest("ML1-P-07-02-01", True, type(indexer) == type(StringIndexer()))
dbTest("ML1-P-07-02-02", True, indexer.getInputCol() == 'species')
dbTest("ML1-P-07-02-03", True, indexer.getOutputCol() == 'speciesClass')

dbTest("ML1-P-07-02-04", True, type(assembler) == type(VectorAssembler()))
dbTest("ML1-P-07-02-05", True, assembler.getInputCols() == irisDF.columns[:-1])
dbTest("ML1-P-07-02-06", True, assembler.getOutputCol() == 'features')

dbTest("ML1-P-07-02-07", True, type(multinomialRegression) == type(LogisticRegression()))
dbTest("ML1-P-07-02-08", True, multinomialRegression.getLabelCol() == "speciesClass")
dbTest("ML1-P-07-02-09", True, multinomialRegression.getFeaturesCol() == 'features')

dbTest("ML1-P-07-02-10", True, type(pipeline) == type(Pipeline()))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Train the Model and Transform the Dataset
# MAGIC 
# MAGIC Train the model and save the results to `multinomialModel` and save the transformed dataset to `irisTransformedDF`.

# COMMAND ----------

# TODO
multinomialModel = pipeline.fit(irisDF)
irisTransformedDF = multinomialModel.transform(irisDF)

# COMMAND ----------

display(irisTransformedDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-07-03-01", True, "prediction" in irisTransformedDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how we have probabilities and raw log odds predictions for all three classes.

# COMMAND ----------

display(irisTransformedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What is the difference between Classification and Regression?  
# MAGIC **Answer:** Classification models a discrete output variable such as whether a patient has cancer.  In this case, there are two possible classes a given observation falls into.  In classification tasks, we predict the probability of a given observations being of a given class.  We then choose a cutoff threshold, such as classifying all values with a probability over 0.5 as being part of a given task.  Regression models continuous variables such as the lifetime value of a customer.  The term _logistic regression_ can be confusing since it's being used for classification.  While we could use linear regression for a less precise way of predicting probabilities, logistic regression bounds the results between 0 and 1.  Combined with the decision rule (that is, the cutoff threshold), logistic regression becomes a classification algorithm.
# MAGIC 
# MAGIC **Question:** How do I interpret the coefficients in logistic regression?  
# MAGIC **Answer:** In the case of linear regression, the coefficient represents the linear increase or decrease in the prediction for a given observed value.  Logistic regression is similar but a bit less intuitive.  In logistic regression, coefficients indicate the change in log odds to our final prediction.  See the walk-through above for details.
# MAGIC 
# MAGIC **Question:** What are the various ways that I can evaluate model performance in binomial classification?  
# MAGIC **Answer:** Evaluating model performance in the case of binomial regression is more complex than in the case of regression, where most metrics are based around the distance between the true and predicted values.  These metrics are different trade-offs between true positives (TP), false positives (FP), true negatives (TN), and false negatives (FN).  Some common metrics include:
# MAGIC 0. Sensitivity = TP / (TP + FN)
# MAGIC 0. Specificity = TN / (TN + FP)
# MAGIC 0. Precision = TP / (TP + FP)
# MAGIC 0. Accuracy = (TP + TN) / (TP + TN + FP + FN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Model Selection]($./08-Model-Selection).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>