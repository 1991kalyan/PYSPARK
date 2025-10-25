from pyspark.sql.functions import lower, col, upper, lit, concat
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("filter").getOrCreate()
df = spark.read.option("header",True).csv(r"C:\Users\Kalya\PycharmProjects\SparkProjects\data\csv\employees_read_mode.csv")
#df.filter(col("SALARY") > lit(5000)).show()
#df.filter((col("SALARY")>=lit(5000)) & (col("SALARY")<=lit(10000))).show()

print()

#df.filter((col("MANAGER_ID")==lit(205)) | (col("DEPARTMENT_ID")==(110))).show()
#df.filter(col("EMPLOYEE_ID").isin(200,206)).show()

df.where("EMPLOYEE_ID ==206")#.show()

df.filter("EMPLOYEE_ID in (206,200,205)")#.show()

df.createOrReplaceTempView("filter_opr")
#df.createOrReplaceTempView("filter_opr")
#df.show()
#
# spark.sql("""
# select * from filter_opr
#     where SALARY >=5000 and SALARY <=1000
#     and EMPLOYEE_ID in (206,200,205)
# """).show()


