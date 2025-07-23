import os
import findspark
findspark.init()

os.environ["JAVA_HOME"] = "C:/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/Spark/spark-3.5.4-bin-hadoop3"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").master("local[*]").getOrCreate()
print("Versión de SparkSession:", spark.version)

df = spark.createDataFrame([(1, "A"), (2, "B")], ["ID", "Valor"])
df.show()

spark.stop()
