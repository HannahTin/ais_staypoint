# -*- coding: UTF-8 -*-
import sys
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from datetime import datetime, timedelta
import math
from math import *
from ais.utils import rad, getDistance, readConf
#利用中值滤波和均值滤波计算原始数据点和滤波后的数据点距离，并与bias偏差比较，来决定是否需要被替换
udfgetDistance = func.udf(getDistance,DoubleType())

def run(loadDate):
    """
    loadDate is the pday to compute
    """
    data = readData(loadDate)
    data = filterAnomarly(data)
    saveData(data, loadDate)


def startSpark():
    spark = SparkSession \
        .builder \
        .appName("Noise Filter on Latitude & Longitude") \
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def readData(loadDate):
    spark = startSpark()
    yestDate = datetime.strptime(loadDate, "%Y-%m-%d") - timedelta(1)
    # print yestDate
    yestDate = yestDate.strftime("%Y-%m-%d")
    # print yestDate
    cfg = readConf()
    database = cfg['data_cfg']['database']
    newDF = spark.sql("SELECT * from %s.mean_median_lat_lon where pday <= '%s' and pday >= '%s'"
            % (database, loadDate, yestDate))
    return newDF


def filterAnomarly(newDF):
    spark = startSpark()
    cfg = readConf()
    THRESHOLD = cfg['algorithm_cfg']['noise_threshold']
    newDF = newDF.withColumn("bias",udfgetDistance(newDF.latitude, newDF.longitude, newDF.lat_mean, newDF.lon_mean))
    newDF = newDF.withColumn("latitude_clean",func.when(newDF.bias<=THRESHOLD, newDF.latitude).otherwise(newDF.lat_median))
    newDF = newDF.withColumn("longitude_clean",func.when(newDF.bias<=THRESHOLD, newDF.longitude).otherwise(newDF.lon_median))
    newDF = newDF.drop("bias").drop("lat_mean").drop("lon_mean").drop("lat_median").drop("lon_median")
    return newDF

def saveData(newDF, loadDate):
    spark = startSpark()
    newDF = newDF.filter("pday = '%s'" % loadDate)
    cfg = readConf()
    database = cfg['data_cfg']['database']
    clean_table = cfg['data_cfg']['clean_table']
    spark.sql("use %s" % database)
    tables = [i.name for i in spark.catalog.listTables()]
    if clean_table in tables:
        spark.sql("alter table %s.%s drop if exists partition(pday='%s')"
                % (database, clean_table, loadDate))
        newDF.write.saveAsTable("%s.%s" % (database, clean_table), partitionBy="pday", mode="append")
    else:
        newDF.write.saveAsTable("%s.%s" % (database, clean_table), partitionBy="pday")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        _, loadDate = sys.argv
        run(loadDate)
    else:
        print >> sys.stderr,  """
        Error! The usage is noise_filter.py <loadDate>
        """
