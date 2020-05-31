from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import count, avg, udf
import datetime

'''
Read a CSV file into a Dataframe
Filter using UDF, regEx
Filter using timestamp
aggregate using SQL-GroupBy

to execute:
spark-submit /home/assamese/work/python-projects/test1/spark_sql_group_by.py
'''

class SparkApp:
    @staticmethod
    def __strip1stChar(s):
        return s[1:]

    @staticmethod
    def run(sqlContext, csv_file_name, input_time, time_interval):
        SparkApp.logger.info("Starting run() !")

        strip1stChar_udf = udf(SparkApp.__strip1stChar)
        df0 = sqlContext.read.format('com.databricks.spark.csv')\
            .options(header='true', inferschema='true')\
            .load(csv_file_name)

        regexForIP = '^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
        df1 = df0.filter(df0['IP'].rlike(regexForIP))

        df2 = df1.withColumn("Time_cleaned", strip1stChar_udf("Time"))\
            .drop("Time")\
            .withColumnRenamed("Staus","Status")
        print('df2Count = {}'.format(df2.count()))
        print(df2.show())

        input_time_plus_time_interval = (datetime.datetime.strptime(input_time, '%d/%b/%Y:%H:%M:%S')
                     + datetime.timedelta(days=time_interval))\
            .strftime(format='%d/%b/%Y:%H:%M:%S')
        print(input_time_plus_time_interval)
        df3 = df2.where(df2.Time_cleaned >= input_time_plus_time_interval)

        print('df3Count = {}'.format(df3.count()))
        print(df3.show())

        #print(df3.groupBy('IP').count().show())
        #print(df3.groupBy("IP", "Status").agg(count("*")).show())
        df3.registerTempTable("df3_table")
        groupByStatus_sql = sqlContext.sql(
        '''
                  SELECT IP, Status, count(Status) AS count_Status
                  FROM df3_table
                  GROUP BY IP, Status
                  order by IP, Status, count_Status
                '''
        )
        print(groupByStatus_sql.show())


if __name__ == "__main__":
    app_name = "df sql example1"
    master_config = "local[3]"  # bin/spark-shell  --master local[N] means to run locally with N threads
    conf = SparkConf().setAppName(app_name).setMaster(master_config)
    sparkContext = SparkContext(conf=conf)
    sparkContext.setLogLevel("INFO")

    log4jLogger = sparkContext._jvm.org.apache.log4j
    SparkApp.logger = log4jLogger.LogManager.getLogger(__name__)

    sqlContext = SQLContext(sparkContext)
    print("------------------------------ " + app_name + " Spark-App-start -----------------------------------------")
    input_time = '28/Nov/2017:06:58:55'  # Analyze logs preceding this timestamp
    time_interval = 2  # s = [2, 3, 5]  # Analyze logs for the past 2, 3, 5 days
    csv_file_name = '/home/assamese/work/python-projects/test1/weblog.csv'
    SparkApp.run(sqlContext, csv_file_name, input_time, time_interval)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
