from pyspark import SparkContext, SparkConf, SQLContext
'''
Read a CSV file into a Dataframe
Print the Dataframe

to execute:
spark-submit /home/assamese/work/python-projects/test1/spark_dfexample1.py
'''
class SparkApp:
    @staticmethod
    def run(sqlContext):
        df1 = sqlContext.read.format('com.databricks.spark.csv')\
            .options(header='true', inferschema='true')\
            .load('/home/assamese/work/python-projects/test1/weblog.csv')

        print(df1.columns)
        print(df1.show())

if __name__ == "__main__":
    app_name = "df example1"
    master_config = "local[3]" # bin/spark-shell  --master local[N] means to run locally with N threads
    conf = SparkConf().setAppName(app_name).setMaster(master_config)
    sparkContext = SparkContext(conf=conf)
    sparkContext.setLogLevel("ERROR")
    sqlContext = SQLContext(sparkContext)
    print("------------------------------ " + app_name + " Spark-App-start -----------------------------------------")

    SparkApp.run(sqlContext)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
