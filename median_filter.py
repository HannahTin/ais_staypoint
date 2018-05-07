# -*- coding: UTF-8 -*-
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.storagelevel import StorageLevel
import numpy as np
from datetime import datetime, timedelta
from ais.utils import median

median_udf = func.udf(median, DoubleType())

def run(loadDate):
    """
    loadDate is the ppday to compute
    """
    data = readData(loadDate)
    data = medianFilter(data)
    for i in range(1,5):
        data = medianNewFilter(data)
    saveData(data, loadDate)

def startSpark():
    spark = SparkSession \
        .builder \
        .appName("Median Filter on Latitude & Longitude") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def readData(loadDate):
    spark = startSpark()
    yestDate = datetime.strptime(loadDate, "%Y-%m-%d") - timedelta(1)
    yestDate = yestDate.strftime("%Y-%m-%d")
    cfg = readConf()
 database = cfg['data_cfg']['database']
    mean_DF = spark.sql("SELECT * FROM %s.mean_lat_lon where pday <= '%s' and pday >= '%s'"
            % (database, loadDate, yestDate))
    mean_DF = mean_DF.repartition(10 * spark._sc.defaultParallelism)
    mean_DF.persist(StorageLevel.DISK_ONLY)
    mean_DF.count()
    return mean_DF


def medianFilter(mean_DF):
    windowSpec = Window.partitionBy(mean_DF['channel_id']).orderBy(mean_DF['timestamp'].asc()).rowsBetween(-4, 4)
    mean_median_DF = mean_DF \
        .withColumn("lat_median", median_udf(func.collect_list("latitude").over(windowSpec))) \
        .withColumn("lon_median", median_udf(func.collect_list("longitude").over(windowSpec)))
    return mean_median_DF


def medianNewFilter(mean_DF):
    spark = startSpark()
    mean_DF = mean_DF.withColumnRenamed("lat_median", "lat1").withColumnRenamed("lon_median", "lon1")
    windowSpec = Window.partitionBy(mean_DF['channel_id']).orderBy(mean_DF['timestamp'].asc()).rowsBetween(-4, 4)
    mean_median_DF = mean_DF \
        .withColumn("lat_median", median_udf(func.collect_list("lat1").over(windowSpec))) \
        .withColumn("lon_median", median_udf(func.collect_list("lon1").over(windowSpec)))
    mean_median_DF = mean_median_DF.drop("lat1").drop("lon1")
    print "after median filter num: %s" % mean_median_DF.count()
    return mean_median_DF
    
    
def saveData(mean_median_DF, loadDate):
    spark = startSpark()
    mean_median_DF = mean_median_DF.filter("pday = '%s'" % loadDate)
    print "after pday filter num: %s" % mean_median_DF.count()
    cfg = readConf()
    database = cfg['data_cfg']['database']
    spark.sql("use %s" % database)
    tables = [i.name for i in spark.catalog.listTables()]
    if "mean_median_lat_lon" in tables:
        spark.sql("alter table %s.mean_median_lat_lon drop if exists partition(pday='%s')"
                % (database, loadDate))
        mean_median_DF.write.saveAsTable("%s.mean_median_lat_lon" % database, partitionBy="pday", mode="append")
    else:
        mean_median_DF.write.saveAsTable("%s.mean_median_lat_lon" % database, partitionBy="pday")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        _, loadDate = sys.argv
        run(loadDate)
    else:
        print >> sys.stderr,  """
        Error! The usage is medisan_filter.py <loadDate>
        """
