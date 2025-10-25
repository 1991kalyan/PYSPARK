from pyspark.sql.functions import lower, col, upper, lit, concat
from pyspark.sql.session import SparkSession
import time

spark = SparkSession.builder.appName("convertcolumn").getOrCreate()
df = spark.read.option("header",True).csv(r"C:\Users\Kalya\PycharmProjects\SparkProjects\data\csv\employees_read_mode.csv")
# list_of_colms=["FIRST_NAME", "LAST_NAME"]
#
# df1 = df.withColumn("JOB_ID", lower(col("JOB_ID")))\
#         .withColumn("FIRST_NAME", upper(col("FIRST_NAME")))\
#         .withColumn("SALARY", col("SALARY") + lit(1000))\
#         .withColumn("FULL_NAME", concat(col("FIRST_NAME"), lit(" "), col("LAST_NAME"))) \
#         .withColumnRenamed("EMPLOYEE_ID", "EMP_ID")
# df1.show()

COLUMNS_TO_KEEP = ["SALARY","EMPLOYEE_ID"]
#From pyspark_column_cleanup.py
all_columns = df.columns

print(all_columns)
for column in COLUMNS_TO_KEEP:
    if column in all_columns:
        print(True)
        df.select(column).show()
    else:
        print(False)

time.sleep(6000)

# columns_to_drop = ["JOB_ID" for "JOB_ID" in all_columns if "JOB_ID" not in COLUMNS_TO_KEEP]
#  This line identifies the columns to be removed.
#  From pyspark_column_cleanup.py
# df_result = df.drop(*columns_to_drop)\
# # # .drop(col("COMMISSION_PCT"), col('PHONE_NUMBER')) \
# # #     .drop(*list_of_colms) \
# # #  \
# # #     df1.show()