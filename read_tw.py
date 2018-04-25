# -*- coding: UTF-8 -*-
import datetime
import os
import pytz
from pyspark import StorageLevel
from pyspark.sql import Row, SparkSession

def startSparkSession():
    """
    启动spark程序
    """
    spark = SparkSession.builder.appName("TW_AIS read data example").enableHiveSupport().getOrCreate()
    return spark


def readData(dataPath, spark):
    """
    读取数据
    """
    sc = spark.sparkContext
    data = sc.textFile(dataPath)
    return data


def transData(line, colName):
    """
    将数据整理成结构化的数据
    """
    cols = []
    # 分解所有数据
    cols += line.split("\t")
    # 提取出timestamp并从其中提取出日期
    timestamp = float(cols[2])
    date = datetime.datetime.fromtimestamp(
        timestamp, pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d")
    tmp = {}
    for i in range(len(colName)):
        tmp[colName[i]] = cols[i]
    tmp["pday"] = date
    # 将数据转换为Row，这样可以使用Spark DataFrame
    return Row(**tmp)


if __name__ == "__main__":
    spark = startSparkSession()
    spark.sql("use ais_data")
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    datadir = '/user/dps-hadoop/taiwan_data/part.taiwan.201612'
    data = readData(datadir, spark)
#    list = os.listdir(rootdir) #列出文件夹下所有的目录与文件
#     for i in range(0,len(list)):
#        path = os.path.join(rootdir,list[i])
#       if os.path.isfile(path):
#        data = readData(path, spark)
#    # 这里需要重新命名列的名字
    colName = [
        "sys_timestamp",
        "mmsi",
        "message_timestamp",
        "nav_status",
        "rot",
        "sog",
        "pos_acc",
        "longitude",
        "latitude",
        "cog",
        "true_head",
        "eta",
        "destid",
        "srcid",
        "distance",
        "speed",
        "draught",
        "ship_type"]
    df = data.map(lambda x: transData(x, colName))
    df = df.repartition(10).toDF()
    df.persist(StorageLevel.DISK_ONLY)
    df.count()
    spark.sql("use ais_data")
    df.write.saveAsTable("original_data_tw", partitionBy="pday", mode="append")
