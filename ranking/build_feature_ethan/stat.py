import argparse
import logging
import os
import sys
import numpy as np
import json
import datetime
import time
from datetime import timedelta
from operator import add, or_, itemgetter
from copy import deepcopy

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

sys.path.append("../../util")
from HashCode import get_flow_num
import tools as sp_tools
import datetime_tools as dt_tools

sys.path.append("feature_extractor")
import user as feature_user
import mobile as feature_mobile
import mltag as feature_mltag

HIST_MAX_LEN = 100
HIST_POS_MIN_LEN = 10
HIST_POS_TRAIN_MIN_LEN = 12
HASH_BUCKET_ITEM = 5000000
HASH_BUCKET_PUB = 1000000

#  信息输入
USER_ACT_VID_DAILY_BASE = "s3://mx-machine-learning/takatak_strategy_statistic/user_action_statistic/video_action_daily"
USER_REG_BASE = "s3://mx-machine-learning/takatak_strategy_statistic/register_data/register_json_all"
VIDEO_FEATURE_7D = "s3://mx-machine-learning/takatak_feature_engineering/video_feature_7d"
#  输出
USER_ACT_DAILY_INTERMEDIATE = "s3://mx-machine-learning/dongxinzhou/takatak/mmoe/user_act_daily_intermediate"
FEATURE_BASE = "s3://mx-machine-learning/dongxinzhou/takatak/mmoe/feature"

def load_user_reg(line):

    def get_age_id(birthday):
        if not birthday:
            return 0
        else:
            this_year = int(dt_tools.get_current_date("%Y"))
            try:
                birth_year = int(birthday.split("-")[0])
            except:
                birth_year = 2000
            age_id = this_year - birth_year
            age_id = 0 if age_id < 0 else age_id
            return (age_id + 1) % feature_user.VOB_BUCKET_AGE

    info = json.loads(line)

    if not info["userid"]:
        return []

    user_reg = {
        "userid": info["userid"],
        "user_gender_id": feature_user.FEAT_GENDER_IDX.get(info.get("gender", ""), 0),
        "user_age_id": get_age_id(info.get("birthday", "")),
    }

    return [json.dumps(user_reg)]

def user_in_range(uuid, user_range):
    if user_range[0] == 0 and user_range[1] == 9999:
        return True
    flow_num = get_flow_num(uuid)
    if flow_num >= user_range[0] and flow_num <= user_range[1]:
        return True
    return False

def udf_uuid_filter(user_range, out_range_sample_rate):

    def uuid_filter_core(uuid, user_range, out_range_sample_rate):
        return user_in_range(uuid, user_range) or np.random.rand() < out_range_sample_rate
    return F.udf(lambda uuid: uuid_filter_core(uuid, user_range, out_range_sample_rate), returnType=BooleanType())


def hash_core(line, bucket):
    if not line:
        return 0
    return get_flow_num(line, module = bucket) + 1

def udf_hash(bucket):
    return F.udf(lambda line: hash_core(line, bucket))

@F.udf(returnType=IntegerType())
def udf_label_finish(cols):
    playtime, duration = float(cols[0]), float(cols[1])
    if (duration > 0 and playtime / duration >= 1.8):
        return 1
    has_action = sum(map(int, cols[2:]))
    if has_action > 0:
        return 1
    if playtime >= 15:
        return 1
    return 0

@F.udf(returnType=FloatType())
def udf_confidence_finish(cols):
    playtime, duration = float(cols[0]), float(cols[1])
    if (duration > 0 and playtime / duration >= 1.8):
        return min(float(np.log(1 + playtime / duration)), 2.0)
    has_action = sum(map(int, cols[2:]))
    if has_action > 0:
        return 1.05
    return 1.0


def build_feature_stat(spark, dates, target_user_range, out_range_sample_rate=0.05):
    # dates: [yyddmm,....]
    # target_user_range:  (start, end)
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    s3_paths = []
    for d in dates:
        s3_path = f"{USER_ACT_VID_DAILY_BASE}/{d}"
        if not sp_tools.s3_check_path(s3_path):
            logging.error(f"can't find s3_path: {s3_path}")
            continue
        s3_paths.append(s3_path)

    df = spark.read.csv(s3_paths, sep = "\001", header = True)
    df = df.filter(udf_uuid_filter(target_user_range, out_range_sample_rate)(df.uuid))
    df = df.select(
        "uuid",
        "timestamp",
        "item_id",
        "item_like",
        "item_share",
        "item_download",
        "item_comment_click",
        "item_comment",
        "publisher_enter",
        "playtime",
        "duration",
        udf_label_finish(F.array(["playtime", "duration", "item_share", "item_download", "item_comment"])).alias("label_finish"),
        udf_confidence_finish(F.array(["playtime","duration", "item_share", "item_download", "item_comment"])).alias("confidence_finish"),
        df.item_like.alias("label_like"),
    )
    df.persist()

    total = df.count()
    total_item = df.select("item_id").distinct().count()
    total_user = df.select("uuid").distinct().count()
    like_count = df.filter("label_like=1").count()
    finish_count = df.filter("label_finish=1").count()
    finish_like_count = df.filter("label_like=1 and label_finish=1").count()

    log_fn = "stat_act_daily-{}_{}".format(dates[-1], dates[0])
    ff = open(log_fn, "w")
    def loggin_print(content):
        print(content)
        print(content, file=ff)

    loggin_print('''
|{}-{}| |
| - | -: | 
| Total Records | {} |
| Total Items | {} |
| Total Users | {} |
| Like  | {} |
| Finish| {} |
|Like & Finish| {} |
    '''.format(dates[-1], dates[0], total, total_item, total_user, like_count, finish_count, finish_like_count))
    
    
    def pt_du_map(row):
        pt, du = float(row["playtime"]), float(row["duration"])
        if du > 0:
            return [(int(pt / du * 100), 1)]
        return []

    def percent_printer(data, desc, max_count=300):
        loggin_print("------------------------------")
        data.sort()
        total_c = sum(map(lambda x: x[1], data))
        loggin_print("Total Count: {}".format(total_c))
        loggin_print(desc)
        loggin_print("| -: | -: | -: | -: |")
        if len(data) > max_count:
            max_left_count = sum(map(lambda x: x[1], data[max_count:]))
            data = data[:max_count]
            data.append((">{}".format(max_count), max_left_count))
        acc = 0
        for p, c in data:
            acc += c
            loggin_print("| {} | {} | {:.2f} | {:.2f} |".format(p, c, c/float(total_c)*100, acc/float(total_c)*100))

    loggin_print("Loop_Percent = playtime / duration * 100%")

    total_loop_percent = df.select("playtime", "duration").rdd.flatMap(pt_du_map).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(total_loop_percent, "# Total Loop Percent \n| Loop_Percent(%) | Count | Percent(%) | Accumulate(%) |")
    
    like_loop_percent = df.filter("label_like=1").select("playtime", "duration").rdd.flatMap(pt_du_map).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(like_loop_percent, "# Like Loop Percent \n| Loop_Percent(%) | Count | Percent(%) | Accumulate(%) |")

    duration_stat = df.select("item_id", df.duration.cast(FloatType())).groupBy("item_id").avg().rdd.map(lambda x: (int(x["avg(duration)"]), 1)).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(duration_stat, "# Item Duration(s) \n| Duration(s) | Count | Percent(%) | Accumulate(%) |")

    popularity_stat = df.select("item_id").withColumn("c", F.udf(lambda x: 1, IntegerType())("item_id")).groupBy("item_id").sum().rdd.map(lambda x: (int(x["sum(c)"]), 1)).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(popularity_stat, "# Item Views \n| View | Count | Percent(%) | Accumulate(%) |")

    playtime_percent = df.select("playtime").rdd.map(lambda x: (int(float(x["playtime"])),1)).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(playtime_percent, "# Playtime(s) Percent \n| Playtime(s) | Count | Percent(%) | Accumulate(%) |")

    pt_conds = ["playtime<5", "playtime>=5 and playtime<10", "playtime>=10 and playtime<15", "playtime>=15 and playtime<20", 
                "playtime>=20 and playtime<30", "playtime>=30 and playtime<60", "playtime>=60"]
    for cond in pt_conds:
        loop_percent = df.filter(cond).select("playtime", "duration").rdd.flatMap(pt_du_map).reduceByKey(lambda x,y: x+y).collect()
        percent_printer(loop_percent, "# {} Loop Percent(%) \n| Loop_Percent | Count | Percent(%) | Accumulate(%) |".format(cond))

    du_conds = ["duration<5", "duration>=5 and duration<10", "duration>=10 and duration<15", "duration>=15 and duration<20", 
                "duration>=20 and duration<30", "duration>=30 and duration<60", "duration>=60"]
    for cond in du_conds:
        loop_percent = df.filter(cond).select("playtime", "duration").rdd.flatMap(pt_du_map).reduceByKey(lambda x,y: x+y).collect()
        percent_printer(loop_percent, "# {} Loop Percent \n| Loop_Percent(%) | Count | Percent(%) | Accumulate(%) |".format(cond))

    popularity_stat = df.select("item_id").withColumn("c", F.udf(lambda x: 1, IntegerType())("item_id")).groupBy("item_id").sum().withColumnRenamed("sum(c)", "interact_count")
    popularity_stat.persist()
    ps_conds = ["interact_count=1", "interact_count<5", "interact_count>=5 and interact_count<10", "interact_count>=10 and interact_count<30", 
                "interact_count>=30 and interact_count<60",  "interact_count>=60"]
    for cond in ps_conds:
        loop_percent = df.join(popularity_stat.filter(cond).select("item_id"), "item_id").select("playtime", "duration").rdd.flatMap(pt_du_map).reduceByKey(lambda x,y: x+y).collect()
        percent_printer(loop_percent, "# {} Loop Percent \n| Loop_Percent(%) | Count | Percent(%) | Accumulate(%) |".format(cond))

    ff.close()
    os.system('aws s3 cp {} {}'.format(log_fn, f"s3://mx-machine-learning/dongxinzhou/takatak/mmoe/feature/stat/{log_fn}"))
    os.system('rm {}'.format(log_fn))


def merge_actions_stat(spark, dates, target_user_range, date):
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    s3_paths = []
    for d in dates:
        s3_path = f"{USER_ACT_DAILY_INTERMEDIATE}/{d}"
        if not sp_tools.s3_check_path(s3_path):
            logging.error(f"can't find s3_path: {s3_path}")
            continue
        s3_paths.append(s3_path)

    df = spark.read.csv(s3_paths, sep=",", header = True)
    # 去掉交总互次数小于10的物品
    view_less_10 = df.select("item_id").withColumn("c", F.udf(lambda x: 1, IntegerType())("item_id")) \
                    .groupBy("item_id").sum().withColumnRenamed("sum(c)", "view_count").filter("view_count<10").drop("view_count")
    df = df.join(view_less_10, "item_id", "left_anti")
    item_count = df.select("item_id").distinct().count()

    def sort_truncation(x):
        uuid, item_hist = x
        item_hist.sort(key=itemgetter(1))
        item_hist = list(map(lambda x: (x[0], x[2], x[3], x[4], x[5]), item_hist))
        # item_hist = item_hist[-HIST_MAX_LEN:]
        return uuid, item_hist

    rdd_all = df.rdd.map(lambda row: (row["uuid"], [(row["item_id"], int(row["timestamp"]), int(row["label_like"]), int(row["label_finish"]), float(row["playtime"]), float(row["duration"]))])).\
                    reduceByKey(lambda x, y: x+y).map(sort_truncation)
    rdd_all.persist()

    log_fn = "stat_action-{}_{}".format(dates[-1], dates[0])
    ff = open(log_fn, "w")
    def loggin_print(content):
        print(content)
        print(content, file=ff)
    loggin_print("# Action Stats {}-{}".format(dates[-1], dates[0]))
    loggin_print("Item Count: {}".format(item_count))

    def print_stats(stats, header):
        loggin_print('''
|{}| |
| - | -: | 
| Count | {} |
| Mean | {} |
| Max | {} |
| Min  | {} |
| Variance | {} |
    '''.format(header, stats.count(), stats.mean(), stats.max(), stats.min(), stats.variance()))

    def percent_printer(data, desc, max_count=100):
        loggin_print("------------------------------")
        data.sort()
        total_c = sum(map(lambda x: x[1], data))
        loggin_print("Total Count: {}".format(total_c))
        loggin_print(desc)
        loggin_print("| -: | -: | -: | -: |")
        if len(data) > max_count:
            max_left_count = sum(map(lambda x: x[1], data[max_count:]))
            data = data[:max_count]
            data.append((">{}".format(max_count), max_left_count))
        acc = 0
        for p, c in data:
            acc += c
            loggin_print("| {} | {} | {:.2f} | {:.2f} |".format(p, c, c/float(total_c)*100, acc/float(total_c)*100))

    loggin_print("# Sequence Length")
    print_stats(rdd_all.map(lambda x: len(x[1])), "Sequence Length Stats")
    seq_len = rdd_all.map(lambda x: (len(x[1]), 1)).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(seq_len, "| Length | Count | Percent(%) | Accumulate(%) |")

    loggin_print("# Positive(Finish=1 or Like=1) Sequence Length")
    print_stats(rdd_all.map(lambda x: sum(list(map(lambda s: 1 if s[1]>0 or s[2]>0 else 0, x[1])))), "Positive(Finish=1 or Like=1) Sequence Length Stats")
    seq_len = rdd_all.map(lambda x: (sum(list(map(lambda s: 1 if s[1]>0 or s[2]>0 else 0, x[1]))), 1)).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(seq_len, "| Length | Count | Percent(%) | Accumulate(%) |")

    loggin_print("# Like=1 Sequence Position")
    like_position = rdd_all.flatMap(lambda x: list(map(lambda p: (p[0], 1), filter(lambda a: a[1][1]>0,  zip(range(len(x[1])), x[1]))))).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(like_position, "| Position | Count | Percent(%) | Accumulate(%) |")

    loggin_print("# Like=1 Positive(Finish=1 or Like=1) Sequence Position")
    like_positive_position = rdd_all.map(lambda x: (x[0], list(filter(lambda s:  s[1]>0 or s[2]>0, x[1])))).flatMap(lambda x: list(map(lambda p: (p[0], 1), filter(lambda a: a[1][1]>0,  zip(range(len(x[1])), x[1]))))).reduceByKey(lambda x,y: x+y).collect()
    percent_printer(like_positive_position, "| Position | Count | Percent(%) | Accumulate(%) |")
    
    rdd_with_pos_len = rdd_all.map(lambda x: (x[0], (sum(list(map(lambda s: 1 if s[1]>0 or s[2]>0 else 0, x[1]))), x[1])))
    pos_len_conds = [(0, 5), (5, 100), (0, 10), (10, 100)]
    for s, e in pos_len_conds:
        pos_loop_percent = rdd_with_pos_len.filter(lambda x: x[1][0]>=s and x[1][0]<e).flatMap(lambda x: map(lambda i: (i[3], i[4]), x[1][1])). \
                        flatMap(lambda x:  [(int(x[0] / x[1] * 100), 1)] if x[1]>0 else []).reduceByKey(lambda x,y: x+y).collect()
        percent_printer(pos_loop_percent, "# {}<=Pos_Length<{} Loop Percent \n| Loop_Percent(%) | Count | Percent(%) | Accumulate(%) |".format(s, e), 300)


    ff.close()
    os.system('aws s3 cp {} {}'.format(log_fn, f"s3://mx-machine-learning/dongxinzhou/takatak/mmoe/feature/stat/{log_fn}"))
    os.system('rm {}'.format(log_fn))
        

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(filename)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    parser = argparse.ArgumentParser(description="argument for build_feature")
    parser.add_argument("--date", type=str)
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--user_range", type=str, default="0_499")
    parser.add_argument("--out_range_sample_rate", type=float, default="0.05")
    parser.add_argument("--stat_type", type=str, default="all",choices=["all", "feature", "action"])
    args = parser.parse_args()

    date = args.date
    if date is None:
        date = datetime.datetime.now().strftime("%Y%m%d")
    user_range = [int(x) for x in args.user_range.split("_")]
    dates = dt_tools.get_date_list_ago(date, args.days+1)[1:]
    print("Dates: {}, User Range: {},  Out Range Sample Rate: {}".format(dates, user_range, args.out_range_sample_rate))

    app_name = "research: TakaTak: build_feature_stat"
    spark = SparkSession.builder.appName(app_name).config("spark.debug.maxToStringFields", 1000).getOrCreate()

    if args.stat_type=="all" or args.stat_type=="feature": 
        build_feature_stat(spark, dates, user_range, args.out_range_sample_rate)
    
    if args.stat_type=="all" or args.stat_type=="action": 
        merge_actions_stat(spark, dates, user_range, args.out_range_sample_rate)
    
    