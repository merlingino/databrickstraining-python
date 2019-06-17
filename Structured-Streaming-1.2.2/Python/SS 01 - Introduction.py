# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
# MAGIC 
# MAGIC # Structured Streaming
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Learn about Structured Streaming at a high level
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Secondary Audience: Data Scientists, Software Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses DataFrames, ETL-Parts 1, 2 and 3 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/mkslslo1zl?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/mkslslo1zl?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Problem</h2>
# MAGIC 
# MAGIC We have a stream of data coming in from a TCP-IP socket, Kafka, Kinesis or other sources...
# MAGIC 
# MAGIC The data is coming in faster than it can be consumed
# MAGIC 
# MAGIC How do we solve this problem?
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/drinking-from-the-fire-hose.png"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Micro-Batch Model</h2>
# MAGIC 
# MAGIC Many APIs solve this problem by employing a Micro-Batch model.
# MAGIC 
# MAGIC In this model, we take our firehose of data and collect data for a set interval of time (the **Trigger Interval**).
# MAGIC 
# MAGIC In our example, the **Trigger Interval** is two seconds.
# MAGIC 
# MAGIC <img style="width:100%" src="https://files.training.databricks.com/images/streaming-timeline.png"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Processing the Micro-Batch</h2>
# MAGIC 
# MAGIC For each interval, our job is to process the data from the previous [two-second] interval.
# MAGIC 
# MAGIC As we are processing data, the next batch of data is being collected for us.
# MAGIC 
# MAGIC In our example, we are processing two seconds worth of data in about one second.
# MAGIC 
# MAGIC <img style="width:100%" src="https://files.training.databricks.com/images/streaming-timeline-1-sec.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### What happens if we don't process the data fast enough when reading from a TCP/IP Stream?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
# MAGIC     <link rel="stylesheet" type="text/css" href="https://files.training.databricks.com/static/assets/spark-ilt/labs.css">
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <div id="the-button"><button style="width:15em" onclick="block('the-answer', 'the-button')">Continue Reading</button></div>
# MAGIC 
# MAGIC     <div id="the-answer" style="display:none">
# MAGIC       <p>In the case of a TCP/IP stream, we will most likely drop packets.</p>
# MAGIC       <p>In other words, we would be losing data.</p>
# MAGIC       <p>If this is an IoT device measuring the outside temperature every 15 seconds, this might be OK.</p>
# MAGIC       <p>If this is a critical shift in stock prices, you could be out thousands of dollars.</p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# MAGIC %md
# MAGIC ### What happens if we don't process the data fast enough when reading from a pubsub system like Kafka?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
# MAGIC     <link rel="stylesheet" type="text/css" href="https://files.training.databricks.com/static/assets/spark-ilt/labs.css">
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <div id="the-button"><button style="width:15em" onclick="block('the-answer', 'the-button')">Continue Reading</button></div>
# MAGIC 
# MAGIC     <div id="the-answer" style="display:none">
# MAGIC       <p>In the case of a pubsub system, it simply means we fall further behind.</p>
# MAGIC       <p>Eventually, the pubsub system would reach resource limits inducing other problems.</p>
# MAGIC       <p>However, we can always re-launch the cluster with enough cores to catch up and stay current.</p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# MAGIC %md
# MAGIC Our goal is simply to process the data for the previous interval before data from the next interval arrives.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> From Micro-Batch to Table</h2>
# MAGIC 
# MAGIC In Apache Spark, we treat such a stream of **micro-batches** as continuous updates to a table.
# MAGIC 
# MAGIC The developer then defines a query on this **input table**, as if it were a static table.
# MAGIC 
# MAGIC The computation on the input table is then pushed to a **results table**.
# MAGIC 
# MAGIC And finally, the results table is written to an output **sink**. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/Delta/stream2rows.png" style="height: 300px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC In general, Spark Structured Streams consist of two parts:
# MAGIC * The **Input source** such as 
# MAGIC   * Kafka
# MAGIC   * Azure Event Hub
# MAGIC   * Files on a distributed system
# MAGIC   * TCP-IP sockets
# MAGIC * And the **Sinks** such as
# MAGIC   * Kafka
# MAGIC   * Azure Event Hub
# MAGIC   * Various file formats
# MAGIC   * The system console
# MAGIC   * Apache Spark tables (memory sinks)
# MAGIC   * The completely custom `foreach()` iterator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Triggers
# MAGIC Developers define **triggers** to control how frequently the **input table** is updated. 
# MAGIC 
# MAGIC Each time a trigger fires, Spark checks for new data (new rows for the input table), and updates the result.
# MAGIC 
# MAGIC From the docs for `DataStreamWriter.trigger(Trigger)`:
# MAGIC > The default value is ProcessingTime(0) and it will run the query as fast as possible.
# MAGIC 
# MAGIC And the process repeats in perpetuity.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> More About Streams</h2>
# MAGIC 
# MAGIC Examples include bank card transactions, log files, Internet of Things (IoT) device data, video game play events and countless others.
# MAGIC 
# MAGIC Some key properties of streaming data include:
# MAGIC * Data coming from a stream is typically not ordered in any way
# MAGIC * The data is streamed into a **data lake**
# MAGIC * The data is coming in faster than it can be consumed
# MAGIC * Streams are often chained together to form a data pipeline
# MAGIC * Streams don't have to run 24/7:
# MAGIC   * Consider the new log files that are processed once an hour
# MAGIC   * Or the financial statement that is processed once a month

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Up and Running with Databricks</h2>
# MAGIC 
# MAGIC Before we continue with Structured Streaming, a little digression on setting up your Databricks account. 
# MAGIC 
# MAGIC You may wish to skip this section if you already have Databricks up and running.
# MAGIC 
# MAGIC Create a notebook and Spark cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** This step requires you to navigate Databricks while doing this lesson.  We recommend you <a href="" target="_blank">open a second browser window</a> when navigating Databricks to view these instructions in one window while navigating in the other.
# MAGIC 
# MAGIC ### Step 1
# MAGIC Databricks notebooks are backed by clusters, or networked computers that work together to process your data. Create a Spark cluster (*if you already have a running cluster, skip to **Step 2** *):
# MAGIC 1. In your new window, click the **Clusters** button in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-4.png" style="height: 200px"/></div><br/>
# MAGIC 2. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 3. Name your cluster. Use your name or initials to easily differentiate your cluster from your coworkers.
# MAGIC 4. Select the cluster type. We recommend the latest runtime and Scala **2.11**.
# MAGIC 5. Specify your cluster configuration.
# MAGIC   * For clusters created on a **Community Edition** shard the default values are sufficient for the remaining fields.
# MAGIC   * For all other environments, refer to your company's policy on creating and using clusters.</br></br>
# MAGIC 6. Right click on **Cluster** button on left side and open a new tab. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-2.png" style="height: 300px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Check with your local system administrator to see if there is a recommended default cluster at your company to use for the rest of the class. This could save you some money!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC Create a new notebook in your home folder:
# MAGIC 1. Click the **Home** button in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/home.png" style="height: 200px"/></div><br/>
# MAGIC 2. Right-click on your home folder.
# MAGIC 3. Select **Create**.
# MAGIC 4. Select **Notebook**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-notebook-1.png" style="height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 5. Name your notebook `First Notebook`.<br/>
# MAGIC 6. Set the language to **Python**.<br/>
# MAGIC 7. Select the cluster to which to attach this notebook.  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If a cluster is not currently running, this option will not exist.
# MAGIC 8. Click **Create**.
# MAGIC <div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2b.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="float:left">&nbsp;&nbsp;&nbsp;or&nbsp;&nbsp;&nbsp;</div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="clear:both"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Now that you have a notebook, use it to run code.
# MAGIC 1. In the first cell of your notebook, type `1 + 1`. 
# MAGIC 2. Run the cell, click the run icon and select **Run Cell**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also run a cell by typing **Ctrl-Enter**.

# COMMAND ----------

1 + 1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Attach and Run
# MAGIC 
# MAGIC If your notebook was not previously attached to a cluster you might receive the following prompt: 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-2.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC 
# MAGIC If you click **Attach and Run**, first make sure you attach to the correct cluster.
# MAGIC 
# MAGIC If it is not the correct cluster, click **Cancel** and follow the steps in the the next cell, **Attach & Detach**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Attach & Detach
# MAGIC 
# MAGIC If your notebook is detached you can attach it to another cluster:  
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <br/>
# MAGIC If your notebook is attached to a cluster you can:
# MAGIC * Detach your notebook from the cluster
# MAGIC * Restart the cluster
# MAGIC * Attach to another cluster
# MAGIC * Open the Spark UI
# MAGIC * View the Driver's log files
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/detach-from-cluster.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Summary</h2>
# MAGIC 
# MAGIC * To create a notebook click the down arrow on a folder and select **Create Notebook**.
# MAGIC * To import notebooks click the down arrow on a folder and select **Import**.
# MAGIC * To attach to a spark cluster select **Attached/Detached**, directly below the notebook title.
# MAGIC * Create clusters using the **Clusters** button on the left sidebar.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Review Questions</h2>
# MAGIC 
# MAGIC **Question:** What is Structured Streaming?<br>
# MAGIC **Answer:** A stream is a sequence of data that is made available over time. Structured Streaming where we treat a <b>stream</b> of data as a table to which data is continously appended. 
# MAGIC 
# MAGIC The developer then defines a query on this input table, as if it were a static table, to compute a final result table that will be written to an output <b>sink</b>. 
# MAGIC .
# MAGIC 
# MAGIC **Question:** How do you create a notebook?  
# MAGIC **Answer:** Sign into Databricks, select the **Home** icon from the sidebar, right-click your home-folder, select **Create**, and then **Notebook**. In the **Create Notebook** dialog, specify the name of your notebook and the default programming language.
# MAGIC 
# MAGIC **Question:** How do you create a cluster?  
# MAGIC **Answer:** Select the **Clusters** icon on the sidebar, click the **Create Cluster** button, specify the specific settings for your cluster and then click **Create Cluster**.
# MAGIC 
# MAGIC **Question:** How do you attach a notebook to a cluster?  
# MAGIC **Answer:** If you run a command while detached, you may be prompted to connect to a cluster. To connect to a specific cluster, open the cluster menu by clicking the **Attached/Detached** menu item and then selecting your desired cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
# MAGIC 
# MAGIC This course is available in Python and Scala.  Start the next lesson, **02-Structured-Streaming-Concepts**.
# MAGIC 1. Click the **Home** icon in the left sidebar
# MAGIC 2. Select your home folder
# MAGIC 3. Select the folder **Structured-Streaming-Version #**
# MAGIC 4. Open the notebook **02-Structured-Streaming-Concepts** in either the Python or Scala folder
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/course-import-2.png" style="margin-bottom: 5px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; width: auto; height: auto; max-height: 350px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
# MAGIC 
# MAGIC **Q:** Where can I find documentation on Structured Streaming?  
# MAGIC **A:** See <a href="https://docs.databricks.com/spark/latest/structured-streaming/index.html" target="_blank">Structured Streaming Guide</a>.
# MAGIC 
# MAGIC **Q:** Are there additional docs I can reference to find my way around Databricks?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>.
# MAGIC 
# MAGIC **Q:** Where can I learn more about the cluster configuration options?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Spark Clusters on Databricks</a>.
# MAGIC 
# MAGIC **Q:** Can I import formats other than .dbc files?  
# MAGIC **A:** Yes, see <a href="https://docs.databricks.com/user-guide/notebooks/index.html#importing-notebooks" target="_blank">Importing notebooks</a>.
# MAGIC 
# MAGIC **Q:** Can I use browsers other than Chrome or Firefox?  
# MAGIC **A:** Databricks is tested for Chrome and Firefox.  It does work on Internet Explorer 11 and Safari, however, it is possible some user-interface features may not work properly.
# MAGIC 
# MAGIC **Q:** Can I install the courseware notebooks into a non-Databricks distribution of Spark?  
# MAGIC **A:** No, the files that contain the courseware are in a Databricks specific format (DBC).
# MAGIC 
# MAGIC **Q:** Do I have to have a paid Databricks subscription to complete this course?  
# MAGIC **A:** No, you can sign up for a free <a href="https://databricks.com/try-databricks" target="_blank">Community Edition</a> account from Databricks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>