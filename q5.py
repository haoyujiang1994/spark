import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("INF551").getOrCreate()


country = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/world").option("dbtable", "country").option("user","inf551").option("password", "inf551").load()
city = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/world").option("dbtable", "city").option("user","inf551").option("password", "inf551").load()

output = country.rdd.map(lambda r : (r.Continent,(r.Population,1))).reduceByKey(lambda (a1,b1),(a2,b2):(a1+a2, b1+b2)).mapValues(lambda r: float(r[0])/r[1]).sortByKey().collect()

for v in output:
    print '%s\t%s' % (v[0], v[1])
