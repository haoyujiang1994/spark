import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("INF551").getOrCreate()


country = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/world").option("dbtable", "country").option("user","inf551").option("password", "inf551").load()
city = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/world").option("dbtable", "city").option("user","inf551").option("password", "inf551").load()

output = country.rdd.map(lambda r : (r.GNP,r.Name)).sortByKey(False).map(lambda r : r[1]).take(1)

for v in output:
    print '%s' % v