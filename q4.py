import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("INF551").getOrCreate()


country = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/world").option("dbtable","country").option("user", "inf551").option("password", "inf551").load()
countrylanguage =spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/world").option("dbtable", "countrylanguage").option("user","inf551").option("password", "inf551").load()


outEng = countrylanguage.rdd.map(lambda r : (r.CountryCode,(r.Language,r.IsOfficial))).\
    filter(lambda r : r[1][0]=="English" and r[1][1] == 'T').\
    map(lambda r : (r[0],r[1])).join(country.rdd.map(lambda r : (r.Code,r.Name))).\
    map(lambda r : r[1][1])

outFre = countrylanguage.rdd.map(lambda r : (r.CountryCode,(r.Language,r.IsOfficial))).\
    filter(lambda r : r[1][0]=="French" and r[1][1] == 'T').\
    map(lambda r : (r[0],r[1])).join(country.rdd.map(lambda r : (r.Code,r.Name))).\
    map(lambda r : r[1][1])

output = outEng.subtract(outFre).map(lambda r : (r,1)).sortByKey().map(lambda r : r[0]).collect()

for v in output:
    print '%s' % v