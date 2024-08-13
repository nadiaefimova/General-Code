
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, dayofweek ,datediff, date_trunc, date_add, floor
from pyspark.sql.types import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno", "from_date","to_date"]
data = [(1, "2022-04-03 12:00:00", "2022-04-03 15:00:00"),
        (2, "2022-04-01 12:00:00", "2022-04-07 15:00:00"),
        (3, "2022-04-01 12:00:00", "2022-04-09 15:00:00"),
        ]

df = spark.createDataFrame(data=data, schema=columns)
#
# #df.show(truncate=False)
df.withColumn("from_date", df["from_date"].cast(TimestampType()))
df.withColumn("to_date", df["to_date"].cast(TimestampType()))
# df.show()

def business_seconds_between(from_date, to_date):
    from_date_dow = dayofweek(from_date)
    to_date_dow = dayofweek(to_date)
    business_seconds = 0
    total_diff_days = datediff(to_date, from_date)

    if from_date_dow == 1:
        new_from_date_unix = unix_timestamp(date_trunc('DD',date_add(from_date,1)))
        weekend_cal_from_date = date_add(from_date, 1)
    elif from_date_dow == 7:
        new_from_date_unix = unix_timestamp(date_trunc('DD',date_add(from_date,2)))
        weekend_cal_from_date = date_add(from_date, 2)
    else:
        new_from_date_unix = unix_timestamp(from_date, format='yyyy-MM-dd HH:mm:ss')
        weekend_cal_from_date = date_trunc('week',from_date)
    if to_date_dow == 1:
        new_to_date_unix = unix_timestamp(date_trunc('DD', date_add(to_date, -1)))
        weekend_cal_to_date = date_add(to_date, -1)
    elif to_date_dow == 7:
        new_to_date_unix = unix_timestamp(date_trunc('DD', date_add(to_date, -2)))
    else:
        new_to_date_unix = unix_timestamp(to_date, format='yyyy-MM-dd HH:mm:ss')
        weekend_cal_to_date = date_trunc('week',to_date)

    total_diff_seconds_cal = new_to_date_unix - new_from_date_unix
    weekend_days = abs(floor(datediff(weekend_cal_to_date,weekend_cal_from_date)))
    if total_diff_seconds_cal <= 0 or (to_date_dow in (1,7) and from_date_dow in (1,7) and total_diff_days <= 1):
        return business_seconds
    else:
        business_seconds = total_diff_seconds_cal - (86400 * weekend_days)
        return business_seconds

spark.udf.register("f_business_seconds_between", business_seconds_between, LongType())

df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select Seqno, from_date,to_date, f_business_seconds_between(from_date, to_date) from NAME_TABLE") \
    .show(truncate=False)