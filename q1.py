import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("INF551").getOrCreate()


country = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/world").option("dbtable", "country").option("user","inf551").option("password", "inf551").load()
city = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/world").option("dbtable", "city").option("user","inf551").option("password", "inf551").load()

output = city.rdd.map(lambda r : (r.CountryCode,r.ID)).groupByKey().\
    mapValues(lambda r: len(r)).filter(lambda r : r[1] > 100).\
    join(country.rdd.map(lambda r : (r.Code,(r.Capital,r.Name)))).\
    map(lambda r : r[1][1]).join(city.rdd.map(lambda r : (r.ID, r.Name))).\
    map(lambda r : r[1]).sortByKey().collect()

for v in output:
    print '%s\t%s' % (v[0], v[1])

