#!/usr/bin/env bash

###################################################
#
# 将编译后的jar包 和配置信息 运行脚本
# 打包成 .tar.gz 并上传至服务器
#
###################################################
#!/bin/bash

#echo "mvn clean package ..."
#
mvn clean package




#部署服务器地址
deploy_server=ali-c-dsj-hadoop02
#模块
module=fire-spark
#版本
version=2.1.0_kafka-0.8
#部署服务器目录 real name authentication
deploy_path=/home/hdfs/deve/test
#用户名
name=gn

echo "scp -P 2222 target/${module}-${version}.tar.gz ${name}@${deploy_server}:$deploy_path"

scp -P 2222 target/${module}-${version}.tar.gz ${name}@${deploy_server}:$deploy_path

#
ssh ${name}@$deploy_server -p 2222 "cd $deploy_path && tar -xzvf ${module}-${version}.tar.gz"
