# -*- encoding:utf-8 -*-
import datetime
import logging
import json
import os
import re
import sys
import time
import requests
try:
    reload(sys)
except:
    from importlib import reload
    reload(sys)
from enum import Enum

try:
    # python2
    from commands import getstatusoutput
except ImportError:
    # python3
    from subprocess import getstatusoutput

def get_datelist(date_dt, days):
    datelist = []
    for span in range(days):
        cur_date_dt = date_dt - datetime.timedelta(span)
        datelist.append(cur_date_dt.strftime("%Y%m%d"))
    return datelist


# ################## datetime end ################

# ################## logging tools beg ################

# set logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(filename)s: %(lineno)d %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def log_debug(msg):
    logging.debug(msg)


def log_info(msg):
    logging.info(msg)


def log_warn(msg):
    logging.warning(msg)


def log_error(msg):
    logging.error(msg)


def log_start(msg, line="*" * 39):
    log_info("{0} {1} {{start}} {0}".format(line, msg))


def log_end(msg, line="*" * 39):
    log_info("{0} {1} {{end}} {0}".format(line, msg))


# ################## logging tools end ################

# ################## s3 commans tools beg #############
def s3_copy_file(src_file, dst_file):
    logging.info("copy file from {} => {}".format(src_file, dst_file))
    status = os.system("/usr/bin/aws s3 cp {} {} --quiet".format(
        src_file, dst_file))
    if status:
        return False
    else:
        return True


def s3_upload_path(loc_path, s3_path, with_remove=False):
    logging.info("upload path from {} => {}".format(loc_path, s3_path))
    if s3_check_path(s3_path) and not with_remove:
        logging.info("s3 path exists {}".format(s3_path))
        return True
    s3_remove_path(s3_path)
    os.system("/usr/bin/aws s3 cp {} {} --recursive --quiet".format(
        loc_path, s3_path))
    return True


def s3_download_path(s3_path, dst_path):
    if "/data/" not in dst_path:
        logging.error("dst_path is error {}".format(dst_path))
        exit(1)
    func_info = "sync s3 path to local {} => {}".format(s3_path, dst_path)
    logging.info(func_info)
    status = os.system("/usr/bin/aws s3 sync {} {} --quiet".format(
        s3_path, dst_path))
    if status:
        return False
    else:
        return True


def s3_copy_path(src_path, dst_path):
    func_info = "copy path {} => {}".format(src_path, dst_path)
    logging.info(func_info)
    status = os.system("/usr/bin/aws s3 cp {} {} --recursive --quiet".format(
        src_path, dst_path))
    if status:
        return False
    else:
        return True


def s3_sync(s3_path, dst_file, gzip=True):
    if "/data/" not in dst_file:
        logging.error("dst_file is error {}".format(dst_file))
        exit(1)
    func_info = "sync s3 path to local {} => {}".format(s3_path, dst_file)
    logging.info(func_info)
    status = os.system("/usr/bin/aws s3 sync {} {}_path --quiet".format(
        s3_path, dst_file))
    if status:
        logging.error(func_info)
        exit(1)
    if gzip:
        status = os.system("zcat {0}_path/*.gz > {0}".format(dst_file))
    else:
        status = os.system("cat {0}_path/* > {0}".format(dst_file))
    os.system("rm -r {}_path".format(dst_file))
    if status:
        return False
    else:
        return True


def s3_check_path(s3_path, success=True):
    logging.info("check s3_path {}".format(s3_path))
    if success:
        status = os.system("/usr/bin/aws s3 ls {}/_SUCCESS".format(s3_path))
    else:
        status = os.system("/usr/bin/aws s3 ls {}".format(s3_path))
    if status:
        return False
    else:
        return True


def s3_check_file(s3_file):
    logging.info("check s3_file {}".format(s3_file))
    status = os.system("/usr/bin/aws s3 ls {}".format(s3_file))
    if status:
        return False
    else:
        return True


TOOLS_S3_PTN = re.compile(r"s3://mx-machine-learning/.*?/.*?/.*\d{8,10}/?")


def s3_ls_path(s3_path):
    s3_path = s3_path.rstrip("/")
    status, output = getstatusoutput("/usr/bin/aws s3 ls {}/".format(s3_path))
    if status != 0:
        return []
    else:
        return [x.strip().split()[-1] for x in output.strip().split("\n")]


def s3_check_deep_path(s3_path, prefix="part-"):
    # 检查s3目录是否是最后一级目录
    # return
    #   True: 是最后一级目录 或者 当前目录还不存在
    #   False: 不是最后一级目录
    s3_file_list = s3_ls_path(s3_path)
    if not s3_file_list:
        return True
    else:
        for f in s3_file_list:
            if f.startswith(prefix) or f == "_SUCCESS":
                return True
        return False


def s3_remove_path(s3_path, s3_base=""):
    if s3_base and not s3_path.startswith(s3_base):
        log_error("can't remove s3 path => {}".format(s3_path))
        exit(0)
    if not re.match(TOOLS_S3_PTN, s3_path):
        log_error("can't remove s3 path => {}".format(s3_path))
        exit(0)
    os.system("/usr/bin/aws s3 rm {} --recursive --quiet".format(s3_path))
    logging.info("remove s3 path success => {}".format(s3_path))


def s3_remove_path_v2(s3_path, s3_base="", prefix="part-"):
    # 通过s3_base检查目录
    if s3_base and not s3_path.startswith(s3_base):
        log_error("can't remove s3 path => {}".format(s3_path))
        exit(0)
    # 检查是否是最后一级目录
    if not s3_check_deep_path(s3_path, prefix = prefix):
        logging.error("can't remove s3 path => {}".format(s3_path))
        exit(0)
    os.system("/usr/bin/aws s3 rm {} --recursive --quiet".format(s3_path))
    logging.info("remove s3 path success => {}".format(s3_path))


def s3_remove_file(s3_file, s3_base=""):
    if s3_base and not s3_file.startswith(s3_base):
        log_error("can't remove s3 file => {}".format(s3_file))
        exit(0)
    if not re.match(TOOLS_S3_PTN, s3_file):
        log_error("can't remove s3 file => {}".format(s3_file))
        exit(0)
    os.system("/usr/bin/aws s3 rm {} --quiet".format(s3_file))
    logging.info("remove s3 file success => {}".format(s3_file))


# ################## s3 commans tools end #############


# ############## rdd operations tools start ################

TOOLS_COMP = {
    "gzip": "org.apache.hadoop.io.compress.GzipCodec",
    "lzo": "com.hadoop.compression.lzo.LzoCodec",
    "bzip2": "org.apache.hadoop.io.compress.Bzip2Codec",
    "zlib": "org.apache.hadoop.io.compress.DefaultCodec",
}


def save_rdd(rdd, save_path, partition=0, comp=None):
    s3_remove_path(save_path)
    log_info("save rdd to s3 path => {}".format(save_path))
    compressionCodecClass = TOOLS_COMP.get(comp, comp)
    if partition:
        rdd.repartition(partition).saveAsTextFile(
            save_path,
            compressionCodecClass=compressionCodecClass)
    else:
        rdd.saveAsTextFile(
            save_path,
            compressionCodecClass=compressionCodecClass)


def save_rdd_v2(rdd, save_path, partition=0, comp=None, prefix="part-"):
    s3_remove_path_v2(save_path, prefix=prefix)
    log_info("save rdd to s3 path => {}".format(save_path))
    compressionCodecClass = TOOLS_COMP.get(comp, comp)
    if partition:
        rdd.repartition(partition).saveAsTextFile(
            save_path,
            compressionCodecClass=compressionCodecClass)
    else:
        rdd.saveAsTextFile(
            save_path,
            compressionCodecClass=compressionCodecClass)


# ############## rdd operations tools end ##################


# ##################### local commands tools beg ################

def loc_check_file(loc_file):
    status = os.system("ls {}".format(loc_file))
    if status:
        return False
    else:
        return True


def loc_build_path(loc_dir, with_remove=False):
    if os.path.isdir(loc_dir):
        if with_remove and loc_dir:
            os.system("rm -r {}/*".format(loc_dir))
            # os.makedirs(loc_dir)
    else:
        os.makedirs(loc_dir)
    return True


def loc_remove_path(loc_dir):
    if "/data/" not in loc_dir:
        log_error("remove local path error => {}".format(loc_dir))
        return False
    if os.path.isdir(loc_dir):
        os.system("rm -r {}".format(loc_dir))
        return True
    else:
        log_error("{} is not dir".format(loc_dir))
        return False


def loc_remove_file(loc_file):
    if "/data/" not in loc_file:
        log_error("remove local file error => {}".format(loc_file))
        return False
    if os.path.isfile(loc_file):
        os.system("rm {}".format(loc_file))
        return True
    else:
        log_error("{} is not file".format(loc_file))
        return False


# ##################### local commands tools end ################



def change_key_lower(_dict):
    return {k.lower(): v for k, v in _dict.items()}

def line_loads(line, sep="\001"):
    cols = line.strip().split(sep)
    return (cols[0], json.loads(cols[1]))


def line_dumps(_id, info, sep="\001"):
    return "{}{}{}".format(_id, sep, json.dumps(info))


def load_publisher_name(publisher_id):
    # url = "http://joyshareud.internal.mxplay.com/v1/user"
    url = "http://joyshareud.internal.mxplay.com/v1/user?ids={}".format(publisher_id)
    params = {"ids": publisher_id}
    # res = requests.get(url, params)
    res = requests.get(url)
    result = json.loads(res.content)
    publisher_name = result.get('res', {}).get(
        publisher_id, {}).get('name', "").encode("utf-8").replace(",", " ").replace("\n", " ")
    return publisher_name


def load_publisher_info(publisher_id):
    def get_res(url, params):
        i = 0
        while i < 3:
            try:
                res = requests.get(url, params, timeout=5).content
                result = json.loads(res)
                return result
            except:
                i += 1
    try:
        url = "https://mxshorts.mxplay.com/v1/publisher"
        params = {"id": publisher_id}
        # res = requests.get(url, params)
        # result = json.loads(res.content)
        result = get_res(url, params)
        follower = int(result.get("follower", 0))
        total_like = int(result.get("total_like", 0))
        return follower, total_like
    except:
        return 0, 0


def parse_duration(content):
    duration = 0
    try:
        if content.get("video_info_resolution_h264"):
            duration = content["video_info_resolution_h264"][0]["duration"] / 1000
        elif content.get("target_video_info"):
            duration = content["target_video_info"]["duration"] / 1000
        else:
            if content["is_ugc_content"] and content.get("qq_mediabasicinfo"):
                duration = content["qq_metadata"]["Duration"]
        return duration
    except:
        return 0


def parse_ugc(info):
    status = info.get("status", 0)
    is_porn = info.get("is_porn", 1)
    is_terrorism = info.get("is_terrorism", 1)
    is_politic = info.get("is_politic", 1)
    if status and not any([is_porn, is_terrorism, is_politic]):
        duration = parse_duration(info)
    else:
        duration = 0
    return duration


def parse_crawler(info):
    status = info.get("status", 0)
    if status:
        duration = parse_duration(info)
    else:
        duration = 0
    return duration


class CDN(Enum):
    AKAMAI = 'Akamai'
    AKAMAI1 = "Akamai1"
    QQ = 'tencent'
    CF = 'cloudfront'
    LIMELIGHT = 'LimeLight'


def hash_vid(vid):
    total = 0
    for i in vid:
        total += ord(i)
    return total


def video_id_cdn(_id):
    h = hash_vid(_id)
    n = h % 100
    cdn = CDN.CF.value
    if n < 20:
        cdn = CDN.QQ.value
    elif n < 40:
        cdn = CDN.LIMELIGHT.value
    elif n < 80:
        cdn = CDN.AKAMAI.value
    return cdn



# ################## spark sql tools start ##############

def df_check(df, name):
    df.createOrReplaceTempView(name)
    df.printSchema()

# ################## spark sql tools end ################
