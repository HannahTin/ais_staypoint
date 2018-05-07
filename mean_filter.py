# -*- coding: UTF-8 -*-
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from datetime import datetime, timedelta

def run(loadDate):
    """
    loadDate is the day to compute
    """
    data = readData(loadDate)
    data = meanFilter(data)
    #多过滤几次
    for i in range(1,5):
        data = meanNewFilter(data)
        saveData(data, loadDate)

def startSpark():
    spark = SparkSession \
        .builder \
        .appName("Mean Filter on Latitude & Longitude") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def readData(loadDate):
    """
    This function will read data of loadDate-1 and loadDate
    """
    spark = startSpark()
    yestDate = datetime.strptime(loadDate, "%Y-%m-%d") - timedelta(1)
    yestDate = yestDate.strftime("%Y-%m-%d")
    cfg = readConf()
database = cfg['data_cfg']['database']
    data = spark.sql("SELECT * FROM %s.data_with_channel_id where pday <= '%s' and pday >= '%s'" % (database, loadDate, yestDate))
    # print "After read data from hive, the number is %s" % data.count()
    return data




def meanFilter(data):
    """
    """
    spark = startSpark()
    windowSpec = Window.partitionBy(data['channel_id']).orderBy(data['timestamp'].asc()).rowsBetween(-4, 4)
    mean_DF = data.withColumn("lat_mean",func.avg(data['latitude']).over(windowSpec)).withColumn("lon_mean",func.avg(data['longitude']).over(windowSpec))
    # print "num of mean is %s" % mean_DF.count()
    return mean_DF


def meanNewFilter(data):
    """
    """
    spark = startSpark()
    windowSpec = Window.partitionBy(data['channel_id']).orderBy(data['timestamp'].asc()).rowsBetween(-4, 4)
    data = data.withColumnRenamed("lat_mean", "lat1").withColumnRenamed("lon_mean", "lon1")
    lat_mean = func.avg(data['lat1']).over(windowSpec)
    lon_mean = func.avg(data['lon1']).over(windowSpec)
    mean_DF = data.withColumn("lat_mean",lat_mean).withColumn("lon_mean",lon_mean)
    mean_DF = mean_DF.drop("lat1").drop("lon1")
    # print "num of mean is %s" % mean_DF.count()
    return mean_DF

def saveData(data, loadDate):
    """
    Save data on loadDate
    """
    spark = startSpark()
    #data = data.repartition(10)
    # 这里先要filter一下，只保存loadDate的数据
    data = data.filter("pday = '%s'" % loadDate)
    cfg = readConf()
    database = cfg['data_cfg']['database']
    spark.sql("use %s" % database)
    tables = [i.name for i in spark.catalog.listTables()]
    if "mean_lat_lon" in tables:
        spark.sql("alter table %s.mean_lat_lon drop if exists partition(pday= '%s')" % (database, loadDate))
        data.write.saveAsTable("%s.mean_lat_lon" % database, partitionBy="pday", mode="append")
    else:
        data.write.saveAsTable("%s.mean_lat_lon" % database, partitionBy="pday")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        _, loadDate = sys.argv
        run(loadDate)
    else:
        print >> sys.stderr,  """
        Error! The usage is mean_filter.py <loadDate>
        """
