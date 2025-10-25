import time

from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("read_csv").getOrCreate()
read_csv_df = spark.read\
    .option("header",True)\
    .option("inferSchema", True)\
    .csv(r"C:\Users\Kalya\PycharmProjects\SparkProjects\data\csv\employees_read_mode.csv")
read_csv_df1 = spark.read\
    .option("header",True)\
    .option("inferSchema", True)\
    .csv(r"C:\Users\Kalya\PycharmProjects\SparkProjects\data\csv\annual-enterprise-survey-2024-financial-year-provisional.csv")
read_csv_df.printSchema()
read_csv_df.show()
read_csv_df1.show()
time.sleep(600)