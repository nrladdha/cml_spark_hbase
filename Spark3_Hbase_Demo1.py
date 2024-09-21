from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
import os

# create jars folder if not exist in the project space

is_jars_exist=os.path.exists('/home/cdsw/jars/')
if not is_jars_exist:
  os.makedirs('/home/cdsw/jars')

#Retrieve required jars from hdfs location and copy to project space

!hdfs dfs -get /tmp/jarsforcml/hbase-site.xml /etc/hadoop/conf/
!hdfs dfs -get /tmp/jarsforcml/jaas.conf /etc/hadoop/conf/
!hdfs dfs -get /tmp/jarsforcml/*.jar /home/cdsw/jars/

# Inform Spark to use authentication as Hbase is secured using kerberos as well as use encryption while interacting with hbase region server

SparkContext.setSystemProperty("spark.security.credentials.hbase.enabled", "true")
SparkContext.setSystemProperty("spark.hbase.rpc.protection","privacy")

# Start new Hbase session if not already available

spark = SparkSession \
   .builder \
   .appName("Spark3.k8s-Hbase") \
   .config("spark.files","/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml,/etc/hadoop/conf/hbase-site.xml,/etc/hadoop/conf/jaas.conf") \
   .getOrCreate()


#Create a sample data/records collection to write to Hbase table

data = [("c001","Alice Alice","alice@alice.com", "london"), ("c002","Bob Bob","bob@bob.com", "San Francisco")]

#Define table schema to initiate dataframe
# tblCustomer has four columns custId, Name, Email and City. custId is a key column

schema = StructType([ \
    StructField("custId",StringType(),True), \
    StructField("name",StringType(),True), \
    StructField("email",StringType(),True), \
    StructField("city", StringType(), True)
  ])

#Create dataframe using sample data and schema define

custDS = spark.createDataFrame(data=data,schema=schema)

#Store data from dataframe to tblCustomer Hbase table using hbase column mapping
# Note that hbase column mapping allows to schema field to Hbase table column.

custDS.write.format("org.apache.hadoop.hbase.spark").option("hbase.columns.mapping", "custId STRING :key, name STRING cf1:name, email STRING cf1:email, city STRING cf1:city").option("hbase.table", "tblCustomer").option("hbase.spark.use.hbasecontext", False).save()


#Read records from the tblCustomer Hbase table to a dataframe using hbase column mapping


df = spark.read.format("org.apache.hadoop.hbase.spark") \
   .option("hbase.columns.mapping",
           "custId STRING :key, name STRING cf1:name, email STRING cf1:email, city STRING cf1:city") \
   .option("hbase.table", "tblCustomer") \
   .option("hbase.spark.use.hbasecontext", False) \
   .load()

# Create a view on the dataframe and read data

df.createOrReplaceTempView("custView")
result = spark.sql("SELECT * FROM custView") # SQL Query
result.show()

spark.stop()
