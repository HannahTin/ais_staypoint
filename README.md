### Cai, Y., Tian, M., Yang, W., & Zhang, Y. (2018). Stay point analysis in automatic identification system trajectory data. Proc. 2018 Int. Con. Data Science, 273-278.

**AIS Data Reading & Cleaning, First & Second Staypoint, Staypoint Clustering on 6000,000,000 pieces of time series data**

This project covers the following steps:

1. **Data Reading and Preprocessing**: Includes mean and median filtering, along with defining `channel_id`.
2. **Staypoint Calculation**:
   - **First Type**: Defined by a speed threshold.
   - **Second Type**: Defined by time and distance thresholds.
3. **Staypoint Clustering**: Performs clustering analysis on staypoints across multiple channels.

### Appendix: Field Descriptions for First Type of Staypoint
| Field Name        | Description                                           |
|-------------------|-------------------------------------------------------|
| `MMSI`            | Unique identifier code for the ship.                  |
| `stay_time`       | Duration of stay.                                     |
| `stay_id`         | Staypoint ID.                                         |
| `threshold`       | Speed threshold (in meters per second; options: 0, 5, 10). |
| `ship_type`       | Type of ship (calculated).                            |
| `Longtitude_clean`| Longitude after data cleaning.                        |
| `Latitude_clean`  | Latitude after data cleaning.                         |
| `stay_start_time` | Start timestamp of staypoint, accurate to the second. |
| `stay_end_time`   | End timestamp of staypoint, accurate to the second.   |
| `pday`            | Date (partition column).                              |

### Field Descriptions for Second Type of Staypoint
| Field Name        | Description                                           |
|-------------------|-------------------------------------------------------|
| `MMSI`            | Unique identifier code for the ship.                  |
| `stay_time`       | Duration of stay.                                     |
| `Longtitude_clean`| Longitude after data cleaning.                        |
| `ship_type`       | Type of ship (calculated).                            |
| `Latitude_clean`  | Latitude after data cleaning.                         |
| `stay_start_time` | Start timestamp of staypoint, accurate to the second. |
| `stay_end_time`   | End timestamp of staypoint, accurate to the second.   |
| `threshold`       | Distance threshold (in meters; options: 10, 50, 20).  |
| `pday`            | Date (partition column).                              |

# ais_staypoint
关于AIS数据的读取和清洗、第一类和第二类停留点的求解，以及停留点聚类分析。

首先对数据进行读取和预处理（包括均值滤波器和中值滤波器及channel_id的定义）。  
其次求解第一类（设定速度阈值）和第二类（设定时间和距离阈值）停留点。  
最后对多条航道的停留点进行聚类分析。

## 第一类停留点字段表示

| 字段名称          | 字段解释                     |
| ---------------- | --------------------------- |
| MMSI             | 船舶MMSI，唯一标识码          |
| stay_time        | 停留时长                     |
| stay_id          | 停留id                       |
| threshold        | 速度阈值（单位为米/秒，可选0, 5, 10）|
| ship_type        | 船舶类型（经过计算后的）       |
| Longtitude_clean | 数据清洗后的经度              |
| Latitude_clean   | 数据清洗后的纬度              |
| stay_start_time  | 停留的起始时间戳，精确到秒     |
| stay_end_time    | 停留的终止时间戳，精确到秒     |
| pday             | 日期，分区列                  |

## 第二类停留点字段表示

| 字段名称          | 字段解释                     |
| ---------------- | --------------------------- |
| MMSI             | 船舶MMSI，唯一标识码          |
| stay_time        | 停留时长                     |
| Longtitude_clean | 数据清洗后的经度              |
| ship_type        | 船舶类型（经过计算后的）       |
| Latitude_clean   | 数据清洗后的纬度              |
| stay_start_time  | 停留的起始时间戳，精确到秒     |
| stay_end_time    | 停留的终止时间戳，精确到秒     |
| threshold        | 距离的阈值（单位米，可选10, 50, 20）|
| pday             | 日期，分区列                  |

