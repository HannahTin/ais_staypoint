# ais_staypoint
About AIS Data reading&cleaning，First&amp;Second staypoint，Staypoint Clustering
首先对数据进行读取和预处理（包括均值滤波器和中值滤波器和channel_id的定义）
其次求解第一类（设定速度阈值）和第二类（设定时间和距离阈值）停留点
最后对多条航道的停留点进行聚类分析

附：第一类停留点字段表示如下
字段名称	字段解释
MMSI	船舶MMSI，唯一标识码
stay_time	停留时长
stay_id  停留id
threshold	速度阈值（单位为米／秒，可选0，5，10）
ship_type	船舶类型（经过计算后的）
Longtitude_clean	数据清洗后的纬度
Latitude_clean	数据清洗后的经度
stay_start_time	停留的起始时间戳，精确到秒
stay_end_time	停留的终止时间戳，精确到秒
pday	日期，分区列

第二类停留点字段表示
字段名称	字段解释
MMSI	船舶MMSI，唯一标识码
stay_time	停留时长
Longtitude_clean	数据清洗后的纬度
ship_type	船舶类型（经过计算后的）
Latitude_clean	数据清洗后的经度
stay_start_time	停留的起始时间戳，精确到秒
stay_end_time	停留的终止时间戳，精确到秒
threshold	距离的阈值（单位米，可选10、50、20）
pday	日期，分区列

