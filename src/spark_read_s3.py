from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import count, avg, udf, split
import datetime
from config_framework import ConfigFramework

'''
Read a CSV file from S3 into a Dataframe
Split 1 column into 2 columns
aggregate using GroupBy

to execute:
spark-submit --conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' --conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4'   --packages org.apache.hadoop:hadoop-aws:2.7.1  /home/assamese/work/python-projects/test1/spark_read_s3.py
'''
class SparkApp:

    @staticmethod
    def run(sqlContext, csv_file_name):
        SparkApp.logger.info("Starting run() !")

        df0 = sqlContext.read.format('com.databricks.spark.csv')\
            .options(header='true', inferschema='true')\
            .load(csv_file_name)
        print(df0.show())
        df1 = df0\
            .withColumn('First_name', split(df0['Customer_name'], ' ')[0])\
            .withColumn('Last_name', split(df0['Customer_name'], ' ')[1])
        print(df1.show())
        df2 = df1.groupBy("Last_name").agg(count("*"))\
            .withColumnRenamed("count(1)", "Unique_last_names")
        print(df2.show())

if __name__ == "__main__":
    app_name = "df read s3"
    master_config = "local[3]"  # bin/spark-shell  --master local[N] means to run locally with N threads
    conf = SparkConf().setAppName(app_name).setMaster(master_config)
    sparkContext = SparkContext(conf=conf)
    sparkContext.setLogLevel("INFO")

    log4jLogger = sparkContext._jvm.org.apache.log4j
    SparkApp.logger = log4jLogger.LogManager.getLogger(__name__)

    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", ConfigFramework.getAWS_Key())
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", ConfigFramework.getAWS_Secret())
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")

    sqlContext = SQLContext(sparkContext)
    print("------------------------------ " + app_name + " Spark-App-start -----------------------------------------")
    bucket_name = 'restaurant-data-sanjay'
    object_key = 'transactions/transactions.csv'
    csv_file_name = "s3a://{}/{}".format(bucket_name, object_key)
    SparkApp.run(sqlContext, csv_file_name)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
