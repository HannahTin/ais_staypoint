# -*- coding: UTF-8 -*-
import yaml
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from ais.utils import *


def run(loadDate):
    data = readData(loadDate)
    data = getChannelId(data)
    saveData(data, loadDate)





def startSpark():
    spark = SparkSession \
        .builder \
        .appName("Adding Channel ID for Data") \
        .enableHiveSupport()\
        .getOrCreate()
    return spark



def readData(loadDate):
    spark = startSpark()
    yestDate = datetime.strptime(loadDate, "%Y-%m-%d") - timedelta(1)
    yestDate = yestDate.strftime("%Y-%m-%d")
    cfg = readConf()
    input_table = cfg['data_cfg']['database'] + "." + cfg['data_cfg']['original_table']
    rowsDF = spark.sql("SELECT * FROM %s ORDER BY timestamp where day <= '%s' and day >= '%s'" % (input_table, loadDate, yestDate))
    return rowsDF


'''
转换channel_id时，利用前后时间戳表示channel改变的状态(0/1)
利用状态和与mmsi的哈希值来对channel_id进行命名
'''
def getChannelId(rowsDF):
    spark = startSpark()
    rowsDF = rowsDF.withColumn("pre_timestamp", func.lag("timestamp").over(Window.partitionBy("mmsi").orderBy("timestamp")))
    rowsDF = rowsDF.withColumn("change_channel", func.when((rowsDF.timestamp - rowsDF.pre_timestamp > 86400), 1).otherwise(0))
    # hash to get channel id
    rowsDF = rowsDF.withColumn("channel_num", func.sum("change_channel").over(Window.partitionBy("mmsi").orderBy("timestamp")))
    rowsDF = rowsDF.withColumn("channel_id", func.hash("mmsi", "channel_num"))
    rowsDF = rowsDF.drop("pre_timestamp").drop("change_channel").drop("channel_num")
    return rowsDF



def saveData(rowsDF, loadDate):
    spark = startSpark()
    # 检测是否已经有表存在
    cfg = readConf()
    database = cfg['data_cfg']['database']
    spark.sql("use %s" % database)
    tables = [i.name for i in spark.catalog.listTables()]
    if "data_with_channel_id" in tables:
        spark.sql("alter table %s.data_with_channel_id drop if exists partition(day= '%s')" % (database, loadDate))
        rowsDF.write.saveAsTable("%s.data_with_channel_id" % database, partitionBy="day", mode="append")
    else:
        rowsDF.write.saveAsTable("%s.data_with_channel_id" % database, partitionBy="day")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        _, loadDate = sys.argv
        run(loadDate)
    else:
        print >> sys.stderr,  """
        Error! The usage is channel_id.py <loadDate>
        """
