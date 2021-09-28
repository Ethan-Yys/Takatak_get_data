import logging
import argparse
import datetime
from pyspark.sql import SparkSession
import datetime_tools as dt_tools

#  信息输入

USER_ACT_VID_DAILY_BASE = "s3://mx-machine-learning/takatak_strategy_statistic/user_action_statistic/video_action_daily"

def daily_active_user_number(spark, dates):
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    num = 0
    for d in dates:
        s3_path = f"{USER_ACT_VID_DAILY_BASE}/{d}"
        df = spark.read.csv(s3_path, sep="\001", header=True)
        df = df.select(
            "uuid",
        )
        df = df.distinct()
        num += df.count()
    return num



if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(filename)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    parser = argparse.ArgumentParser(description="argument for build_feature")
    parser.add_argument("--date", type=str)
    parser.add_argument("--days", type=int, default=1)
    args = parser.parse_args()

    date = args.date
    if date is None:
        date = datetime.datetime.now().strftime("%Y%m%d")
    dates = dt_tools.get_date_list_ago(date, args.days+1)[1:]
    print("Dates: {}".format(dates))
    app_name = "research: TakaTak: daily_active_user_num"
    spark = SparkSession.builder.appName(app_name).config("spark.debug.maxToStringFields", 1000).getOrCreate()
    num = daily_active_user_number(spark, dates)
    print("Daily_active_user_num: {}".format(num))

