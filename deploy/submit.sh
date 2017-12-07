#!/usr/bin/env bash
. /etc/profile
### 脚本所在目录
base=$(cd $(dirname $0);pwd)
### 脚本名称
base_name=$(basename $0)
### 依赖lib目录
lib_path=$base/lib

PROP=$1
[ ! -f "$base/$PROP" ] && { echo "Usage: base $0 config_file";exit; }
PROP="$base/$PROP"
##########################变量配置区域###################################

### 执行代码的jar包
MAIN_JAR="$lib_path/$(basename $base).jar"
#
MAIN=$(grep 'spark.main.class' $PROP | grep -v '^#' | awk -F'=' '{print $2}')

MAIN_PARAMS=$(grep 'spark.main.params' $PROP | grep -v '^#' | awk -F'params=' '{print $2}')

spark_appname=$(grep 'spark.app.name' $PROP | grep -v '^#' | awk -F'=' '{print $2}')

if [ "x$2" != "x" ]; then
    MAIN_PARAMS=$2
    echo "$MAIN_PARAMS"
fi

##########################变量配置区域结束################################

### 组装第三方依赖jar
for jar in $(ls $lib_path)
do
    if [ "x$jar" != "x$(basename $MAIN_JAR)" ]; then
        jars=$lib_path/$jar,$jars
    fi
done
test "x$jars" != "x" && jars="--jars ${jars%,}"



#####################################################################################################################

yarn application --list | grep -i "spark" | grep "${spark_appname}"

if [ $? -eq 0 ]; then
    printf "\n\n\nSparkApp (${spark_appname}): 已存在\n\n\n"
else
    printf "\n\n\nSparkApp (${spark_appname}): 正在启动 ... ... \n\n\n"




sudo -u hdfs spark2-submit \
    --queue spark \
    $jars \
    --properties-file ${PROP} \
    --class ${MAIN} ${MAIN_JAR} "${MAIN_PARAMS}" "${USER_PARAMS}"

