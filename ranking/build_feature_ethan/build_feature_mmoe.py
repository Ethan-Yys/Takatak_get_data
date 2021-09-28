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
HIST_POS_MIN_LEN = 5
HIST_POS_TRAIN_MIN_LEN = 5
HASH_BUCKET_ITEM = 2000000
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


@F.udf(returnType=IntegerType())
def udf_user_state_id(clientip_state):
    if clientip_state is None:
        return 0
    return feature_user.FEAT_STATE_IDX.get(clientip_state, 0)

def hash_core(line, bucket):
    if not line:
        return 0
    return get_flow_num(line, module = bucket) + 1

def udf_hash(bucket):
    return F.udf(lambda line: hash_core(line, bucket))

@F.udf(returnType=IntegerType())
def udf_label_finish(cols):
    playtime, duration = float(cols[0]), float(cols[1])
    if duration > 0:
        if duration < 10 and  playtime / duration >= 2.0:
            return 1
        elif duration > 20 and  playtime / duration >= 1.5:
            return 1
        elif playtime / duration >= 1.8:
            return 1
    has_action = sum(map(int, cols[2:]))  # share, download, comment
    if has_action > 0:
        return 1
    # if playtime >= 15:
    #    return 1
    return 0

@F.udf(returnType=FloatType())
def udf_confidence_finish(cols):
    is_finish, playtime, duration = int(cols[0]), float(cols[1]), float(cols[2])
    if is_finish > 0 and duration > 0:
        return min(max(1.0, float(np.log(1 + playtime / duration))), 2.0)
    has_action = sum(map(int, cols[3:]))
    if has_action > 0:
        return 1.05
    return 1.0


def udf_label_fields():
    label_list = F.array([
        "item_action",
        "playtime",
        "duration",
    ])
    return label_list


@F.udf(returnType=StringType())
def udf_set_userid(userid):
    if not userid:
        userid = "fake_" + str(np.random.randint(0, 100000))
    return userid

def build_feature(spark, dates, target_user_range, out_range_sample_rate=0.05):
    # dates: [yyddmm,....]
    # target_user_range:  (start, end)
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    rdd_user_reg = sc.textFile(USER_REG_BASE).flatMap(load_user_reg)
    df_user_reg = spark.read.json(rdd_user_reg)

    df_video_feature_7d = spark.read.csv(f"{VIDEO_FEATURE_7D}/{dates[-1]}", header = True)
    video_7d_features = {
        "list_ctr_7d": 0,
        "play_rate_7d": 0,
        "finish_rate_7d": 0,
        "loop_play_rate_7d":0,
        "finish_rate_15s_cut_7d": 0,
        "finish_retention_sum_7s_7d": 0,
    }
    df_video_feature_7d = df_video_feature_7d.select(
        "video_id",
        *list(video_7d_features.keys())
    )

    vid_act_paths = []
    for d in dates:
        s3_path = f"{USER_ACT_VID_DAILY_BASE}/{d}"
        if not sp_tools.s3_check_path(s3_path):
            logging.error(f"can't find s3_path: {s3_path}")
            continue
        vid_act_path = f"{USER_ACT_DAILY_INTERMEDIATE}/{d}"
        vid_act_paths.append(vid_act_path)
        if sp_tools.s3_check_path(vid_act_path):
            continue

        df = spark.read.csv(s3_path, sep = "\001", header = True)
        
        df = df.filter(udf_uuid_filter(target_user_range, out_range_sample_rate)(df.uuid))
        df = df.select(
            "uuid",
            udf_set_userid(df.userid).alias("userid"),
            udf_user_state_id("clientip_state").alias("user_state_id"),
            udf_hash(feature_mobile.HASH_BUCKET_MODEL)("model").alias("user_model_id"),
            "timestamp",
            "item_id",
            "publisher",
            "ca_ml_tags",
            "item_like",
            "item_share",
            "item_download",
            "item_comment_click",
            "item_comment",
            "publisher_enter",
            "playtime",
            "duration",
            udf_user_state_id("ca_state").alias("item_upload_state_id"),
            udf_label_finish(F.array(["playtime", "duration", "item_share", "item_download", "item_comment"])).alias("label_finish"),
            F.udf(lambda is_like: 1 if int(is_like)==1 else 0, returnType=IntegerType())(df.item_like).alias("label_like")
        )
        df = df.withColumn("confidence_finish", udf_confidence_finish(F.array(["label_finish", "playtime","duration", "item_share", "item_download", "item_comment"])))

        df = df.join(df_user_reg, "userid", "leftouter").fillna({"user_gender_id": 0, "user_age_id": 0}).drop("userid")
        df = df.join(df_video_feature_7d, df.item_id == df_video_feature_7d.video_id, "leftouter").fillna(video_7d_features).drop("video_id")
        df = df.repartition(1000, "uuid")
        df.write.mode("overwrite").csv(vid_act_path, header=True, compression="gzip")

    return vid_act_paths

def get_item_mltag_id(mltag_line):
    if mltag_line is None:
        return 0
    mltag_list = mltag_line.strip().split("\002")
    if len(mltag_list) <= 0:
        return 0
    mltag_idf_list = [(mltag, feature_mltag.FEAT_MLTAG_IDF_MAP.get(mltag, 0))
                        for mltag in mltag_list]
    mltag_idf_max = max(mltag_idf_list, key=itemgetter(1))[0]
    return feature_mltag.FEAT_MLTAG_IDX.get(mltag_idf_max, 0)

def extract_action(row):
    user_feat = {
        "user_gender_id": int(row["user_gender_id"]),
        "user_age_id": int(row["user_age_id"]),
        "user_state_id": int(row["user_state_id"]),
        "user_model_id": int(row["user_model_id"]),
    }
    item_feat = [{
        "item_id": hash_core((row["item_id"]), HASH_BUCKET_ITEM),
        "timestamp": int(row["timestamp"]),
        "label_finish": int(row["label_finish"]),
        "confidence_finish": float(row["confidence_finish"]),
        "label_like": int(row["label_like"]),
        "publisher": hash_core((row["publisher"]), HASH_BUCKET_PUB),
        "ca_ml_tags": get_item_mltag_id(row["ca_ml_tags"]),
        "item_upload_state": int(row["item_upload_state_id"]),
        "item_duration": min(float(row["duration"]) / 60, 1.0),
        "item_like": int(row["item_like"]),
        "item_share": int(row["item_share"]),
        "item_download": int(row["item_download"]),
        "item_comment_click": int(row["item_comment_click"]),
        "item_comment": int(row["item_comment"]),
        "publisher_enter": int(row["publisher_enter"]),
        "item_loops": min(float(row["playtime"]) / max(1, float(row["duration"])), 10),
        "item_interact_hour": time.localtime(int(row["timestamp"])).tm_hour,
        "item_interact_days_ago": max(0, (time.time() - float(row["timestamp"]))/(24*3600)),
        "list_ctr_7d": float(row["list_ctr_7d"]),
        "play_rate_7d": float(row["play_rate_7d"]),
        "finish_rate_7d": float(row["finish_rate_7d"]),
        "loop_play_rate_7d": float(row["loop_play_rate_7d"]),
        "finish_rate_15s_cut_7d": float(row["finish_rate_15s_cut_7d"]),
        "finish_retention_sum_7s_7d": float(row["finish_retention_sum_7s_7d"]),
    }]
    return (row["uuid"], [user_feat, item_feat])

def merge_action(x, y):
    user_feat = x[0] if x[0]["user_state_id"] else y[0]
    item_hist = x[1] + y[1]
    return [user_feat, item_hist]

def build_samples(line, target_user_range, feature_stat):
    uuid = line[0]
    user_feat, hist_items = line[1]
    is_user_in_range = user_in_range(uuid, target_user_range)

    def is_pos_item(item):
        return item["label_finish"] == 1 or item["label_like"] == 1

    pos_item_count = sum([1 for item in hist_items if is_pos_item(item)])
    feature_stat["user_counter"] += 1

    train_samples = []
    test_samples = []
    predict_samples = []

    seq_item_features = {
        "item_id": ("hist_item_id", []), 
        "publisher": ("hist_pub_id", []),
        "ca_ml_tags": ("hist_item_mltag", []),
        "item_upload_state": ("hist_item_upload_state", []),
        "item_duration": ("hist_item_duration", []),
        "list_ctr_7d": ("hist_list_ctr_7d", []),
        "play_rate_7d": ("hist_play_rate_7d", []),
        "finish_rate_7d": ("hist_finish_rate_7d", []),
        "loop_play_rate_7d": ("hist_loop_play_rate_7d", []),
        "finish_rate_15s_cut_7d": ("hist_finish_rate_15s_cut_7d", []),
        "finish_retention_sum_7s_7d": ("hist_finish_retention_sum_7s_7d", []),
        "item_like": ("h_item_like", []),
        "item_share": ("h_item_share", []),
        "item_download": ("h_item_download", []),
        "item_comment_click": ("h_item_comment_click", []),
        "item_comment": ("h_item_comment", []),
        "publisher_enter": ("h_publisher_enter", []),
        "item_loops": ("h_item_loops", []),
        "item_interact_hour": ("h_item_interact_hour", []),
        "item_interact_days_ago": ("h_item_interact_days_ago", []),
        "timestamp": ("h_item_interact_timestamp", []),
    }

    def add_item_feature(item):
        item_feature = deepcopy(user_feat)
        item_feature.update({n: "\002".join(v) for _, (n, v) in seq_item_features.items()})
        item_feature.update({
            "item_id": item["item_id"],
            "pub_id": item["publisher"],
            "item_mltag": item["ca_ml_tags"],
            "item_upload_state": item["item_upload_state"],
            "item_duration": item["item_duration"],
            "list_ctr_7d": item["list_ctr_7d"],
            "play_rate_7d": item["play_rate_7d"],
            "finish_rate_7d": item["finish_rate_7d"],
            "loop_play_rate_7d": item["loop_play_rate_7d"],
            "finish_rate_15s_cut_7d": item["finish_rate_15s_cut_7d"],
            "finish_retention_sum_7s_7d": item["finish_retention_sum_7s_7d"],
            "label_finish": item["label_finish"], 
            "label_like": item["label_like"],
            "confidence_finish": item["confidence_finish"]
        })
        return item_feature
    
    for item in hist_items:
        if len(seq_item_features["item_id"][1]) >= HIST_POS_TRAIN_MIN_LEN:
            # 范围之外的用户，倒数第二个正样本之后的样本作为测试，不过3天的测试样本数量可能达到600w+，这里进行一些采样，正样本1.5%，负样本1%
            if not is_user_in_range and len(seq_item_features["item_id"][1]) == pos_item_count-1:
                if is_pos_item(item):
                    if np.random.rand() < 0.015:
                        test_samples.append(json.dumps(add_item_feature(item)))
                        feature_stat["test_counter"] += 1
                        feature_stat["test_pos_counter"] += 1
                        if item["label_finish"] == 1: 
                            feature_stat["test_finish_counter"] += 1
                        if item["label_like"] == 1:
                            feature_stat["test_like_counter"] += 1
                elif np.random.rand() < 0.01:
                    test_samples.append(json.dumps(add_item_feature(item)))
                    feature_stat["test_counter"] += 1
            # 交互序列长度足够，加入训练集
            else:
                train_samples.append(json.dumps(add_item_feature(item)))
                feature_stat["train_counter"] += 1
                if is_pos_item(item): 
                    feature_stat["train_pos_counter"] += 1
                    if item["label_finish"] == 1: 
                        feature_stat["train_finish_counter"] += 1
                    if item["label_like"] == 1:
                        feature_stat["train_like_counter"] += 1
        if is_pos_item(item):
            for k in seq_item_features:
                seq_item_features[k][1].append(str(item[k]))
            
    # 范围内的用户， 加入预测集
    if is_user_in_range:
        item_feature = deepcopy(user_feat)
        item_feature.update({n: "\002".join(v) for _, (n, v) in seq_item_features.items()})
        item_feature["uuid_raw"] = uuid
        predict_samples.append(json.dumps(item_feature))
        feature_stat["predict_counter"] += 1

    return [(train_samples, test_samples, predict_samples)]

def get_schema(is_predict=False):
    schema = StructType()
    # 用户特征列
    schema.add("user_age_id", data_type=IntegerType(), nullable=False)
    schema.add("user_gender_id", data_type=IntegerType(), nullable=False)
    schema.add("user_state_id", data_type=IntegerType(), nullable=False)
    schema.add("user_model_id", data_type=IntegerType(), nullable=False)
    # 历史交互特征列
    schema.add("hist_item_id", data_type=StringType(), nullable=False)
    schema.add("hist_pub_id", data_type=StringType(), nullable=False)
    schema.add("hist_item_mltag", data_type=StringType(), nullable=False)
    schema.add("hist_item_upload_state", data_type=StringType(), nullable=False)
    schema.add("hist_item_duration", data_type=StringType(), nullable=False)
    schema.add("hist_list_ctr_7d", data_type=StringType(), nullable=False)
    schema.add("hist_play_rate_7d", data_type=StringType(), nullable=False)
    schema.add("hist_finish_rate_7d", data_type=StringType(), nullable=False)
    schema.add("hist_loop_play_rate_7d", data_type=StringType(), nullable=False)
    schema.add("hist_finish_rate_15s_cut_7d", data_type=StringType(), nullable=False)
    schema.add("hist_finish_retention_sum_7s_7d", data_type=StringType(), nullable=False)
    # 只存在于历史交互物品的特征列
    schema.add("h_item_like", data_type=StringType(), nullable=False)
    schema.add("h_item_share", data_type=StringType(), nullable=False)
    schema.add("h_item_download", data_type=StringType(), nullable=False)
    schema.add("h_item_comment_click", data_type=StringType(), nullable=False)
    schema.add("h_item_comment", data_type=StringType(), nullable=False)
    schema.add("h_publisher_enter", data_type=StringType(), nullable=False)
    schema.add("h_item_loops", data_type=StringType(), nullable=False)
    schema.add("h_item_interact_hour", data_type=StringType(), nullable=False)
    schema.add("h_item_interact_days_ago", data_type=StringType(), nullable=False)
    schema.add("h_item_interact_timestamp", data_type=StringType(), nullable=False)
    if not is_predict:
        # 目标物品特征列
        schema.add("item_id", data_type=IntegerType(), nullable=False)
        schema.add("pub_id", data_type=IntegerType(), nullable=False)
        schema.add("item_mltag", data_type=IntegerType(), nullable=False)
        schema.add("item_upload_state", data_type=IntegerType(), nullable=False)
        schema.add("item_duration", data_type=FloatType(), nullable=False)
        schema.add("list_ctr_7d", data_type=FloatType(), nullable=False)
        schema.add("play_rate_7d", data_type=FloatType(), nullable=False)
        schema.add("finish_rate_7d", data_type=FloatType(), nullable=False)
        schema.add("loop_play_rate_7d", data_type=FloatType(), nullable=False)
        schema.add("finish_rate_15s_cut_7d", data_type=FloatType(), nullable=False)
        schema.add("finish_retention_sum_7s_7d", data_type=FloatType(), nullable=False)
        # 标签特征列
        schema.add("label_finish", data_type=IntegerType(), nullable=False)
        schema.add("confidence_finish", data_type=FloatType(), nullable=False)
        schema.add("label_like", data_type=IntegerType(), nullable=False)
    else:
        schema.add("uuid_raw", data_type=StringType(), nullable=False)
    return schema

def merge_actions(spark, vid_act_paths, target_user_range, date):
    df = spark.read.csv(vid_act_paths, sep=",", header = True)
    # 去掉交总互次数小于10的物品
    view_less_10 = df.select("item_id").withColumn("c", F.udf(lambda x: 1, IntegerType())("item_id")) \
                    .groupBy("item_id").sum().withColumnRenamed("sum(c)", "view_count").filter("view_count<10").drop("view_count")
    df = df.join(view_less_10, "item_id", "left_anti")
    item_count = df.rdd.map(lambda x: x["item_id"]).distinct().count()
    # pub_count = df.rdd.map(lambda x: x["publisher"]).distinct().count()
    print("Item Count: {}".format(item_count))
    def sort_truncation(x):
        user_feat, item_hist = x[1]
        item_hist.sort(key=itemgetter("timestamp"))
        item_hist = item_hist[-HIST_MAX_LEN:]
        return x[0], [user_feat, item_hist]

    rdd_all = df.rdd.map(extract_action).reduceByKey(merge_action).map(sort_truncation)
    # 用户正样本最小长度过滤
    rdd_all = rdd_all.filter(lambda line: sum([1 if x["label_finish"]>0 or x["label_like"]>0 else 0 for x in line[1][1]]) >= HIST_POS_MIN_LEN)

    feature_stat = {
        "train_counter": spark.sparkContext.accumulator(0),
        "train_pos_counter": spark.sparkContext.accumulator(0),
        "train_finish_counter": spark.sparkContext.accumulator(0),
        "train_like_counter": spark.sparkContext.accumulator(0),
        "test_counter": spark.sparkContext.accumulator(0),
        "test_pos_counter": spark.sparkContext.accumulator(0),
        "test_finish_counter": spark.sparkContext.accumulator(0),
        "test_like_counter": spark.sparkContext.accumulator(0),
        "predict_counter": spark.sparkContext.accumulator(0),
        "user_counter": spark.sparkContext.accumulator(0),
    }

    rdd_feature = rdd_all.flatMap(lambda x: build_samples(x, target_user_range, feature_stat))
    rdd_feature.persist()

    rdd_feature_train = rdd_feature.flatMap(lambda line: line[0])
    df_train = spark.read.json(rdd_feature_train, schema=get_schema()).coalesce(100)
    train_path = f"{FEATURE_BASE}/{date}/train"
    logging.info(train_path)
    df_train.write.mode("overwrite").csv(train_path, header=True, compression="gzip")

    rdd_feature_test = rdd_feature.flatMap(lambda line: line[1])
    df_test = spark.read.json(rdd_feature_test, schema=get_schema()).coalesce(10)
    test_path = f"{FEATURE_BASE}/{date}/test"
    logging.info(test_path)
    df_test.write.mode("overwrite").csv(test_path, header=True, compression="gzip")

    # 只为最近一天的用户预测
    user_last_day = spark.read.csv(vid_act_paths[0], sep=",", header = True).select("uuid").distinct()
    user_last_day = user_last_day.filter(F.udf(lambda uuid: user_in_range(uuid, target_user_range), returnType=BooleanType())(user_last_day.uuid))
    user_last_day = user_last_day.withColumnRenamed("uuid", "uuid_raw")

    rdd_feature_predict = rdd_feature.flatMap(lambda line: line[2])
    df_predict = spark.read.json(rdd_feature_predict, schema=get_schema(True)).join(user_last_day, "uuid_raw").coalesce(100)
    # df_predict = spark.read.json(rdd_feature_predict, schema=get_schema(True)).coalesce(100)
    predict_user_count = df_predict.count()

    predict_path = f"{FEATURE_BASE}/{date}/predict"
    logging.info(predict_path)
    df_predict.write.mode("overwrite").csv(predict_path, header=True, compression="gzip")

   
    stat_fn = "feature_stat"
    for k in feature_stat:
        feature_stat[k] = feature_stat[k].value
    feature_stat["train_pos_ratio"] = "{:.4f}".format(feature_stat["train_pos_counter"]/feature_stat["train_counter"])
    feature_stat["train_finish_ratio"] = "{:.4f}".format(feature_stat["train_finish_counter"]/feature_stat["train_counter"])
    feature_stat["train_like_ratio"] = "{:.4f}".format(feature_stat["train_like_counter"]/feature_stat["train_counter"])
    feature_stat["actual_predict_counter"] = predict_user_count
    with open(stat_fn, 'w') as f:
        for k, c in feature_stat.items():
            f.write("{}: {}\n".format(k, c))
            print("{}: {}".format(k, c))
    os.system('aws s3 cp {} {}'.format(stat_fn, f"{FEATURE_BASE}/{date}/{stat_fn}"))
    os.system('rm {}'.format(stat_fn))


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
    args = parser.parse_args()

    date = args.date
    if date is None:
        date = datetime.datetime.now().strftime("%Y%m%d")
    user_range = [int(x) for x in args.user_range.split("_")]
    dates = dt_tools.get_date_list_ago(date, args.days+1)[1:]
    print("Dates: {}, User Range: {},  Out Range Sample Rate: {}".format(dates, user_range, args.out_range_sample_rate))

    app_name = "research: TakaTak: build_feature"
    spark = SparkSession.builder.appName(app_name).config("spark.debug.maxToStringFields", 1000).getOrCreate()

    vid_act_paths = build_feature(spark, dates, user_range, args.out_range_sample_rate)
    print("Intermediate Process Down")
    merge_actions(spark, vid_act_paths, user_range, date)
