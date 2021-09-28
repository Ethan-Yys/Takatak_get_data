
if [ $# -eq 1 ]; then
    YMD=$1
else
    YMD=$(date +%Y%m%d -u)
fi

# 加载shell tools辅助函数
source ../../spark_util/shell_tools.sh


days=1
user_range=0_499
out_range_sample_rate=0.03

JOB_PREFIX="research::TakaTak: build_feature_test"


function check_cmd(){
    if [ $1 -ne 0 ];then
        exit $1
    fi
}

# 训练样本，测试数据集，用户历史特征提取
bash ../../spark_util/spark_submit.sh "${JOB_PREFIX}" \
    2h 2g 500 1 4g 1000 4000 \
    build_feature_test.py \
    "--date ${YMD} --days ${days} --user_range ${user_range}  --out_range_sample_rate ${out_range_sample_rate}" \
    "../../util/*.py,feature_extractor/*.py"
check_cmd $?



