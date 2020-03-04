#!/usr/bin/env bash
base=$(cd $(dirname $0);pwd)

function my_include(){
    local f=${1:-"$HOME/.bash_profile"}
    test -f $f && . $f
}
function check_cmd(){
	local cmd=${1:-"java"}
	if which $cmd >/dev/null 2>&1;then
		echo 1
	else
		echo 0
	fi
}
function get_abs_path(){
        local  f=$1
        test "x$f" == "x" && return
        test ! -f "$f" && test ! -d "$f" && { echo $f;return; }
        local dir=$(cd $(dirname $f);pwd)
        echo "$dir/$(basename $f)"
}
function check_env(){
        local var=$1
        local d=$2
        if [ "x${!var}" == "x" ] && [ "x$d" != "x" ];then
                eval "export $var=$d"
		else
			test "x${!var}" == "x" && "x$d" == "x" && {
				echo "env variable $var not set." >&2;
				exit;
			}
        fi
}
function get_param(){
	local var=$1
        local n=$2
        local d=$3

	test ! -f "$user_proper_file" && { echo "Properties $user_proper_file file not set">&2;exit; }
        local v=$(grep "^$n=" $user_proper_file | head -1 | awk -F '=' '{s="";for(i=2;i<=NF;i++){if(s){s=s"="$i}else{s=$i}};print s}')
        test "x$v" == "x" && test "x$d" != "x" && v="$d"
        test "x$v" == "x" && { echo "$n not set">&2;exit; }
        eval "$var=\"$v\""
}
function set_jars(){
	local jar=""
	test ! -d "$lib_path" && { echo "lib_path $lib_path not found.">&2;exit; }
	for jar in $(ls $lib_path)
	do
	    if [ "x$jar" != "x$(basename $main_jar)" ] && [ "${jar##*.}"x = "jar"x ]; then
	        jars=$lib_path/$jar,$jars
	    fi
	done
	test "x$jars" != "x" && jars="--jars ${jars%,}"
}
function check_command(){
	local c=${1:-"yarn"}
	if ! which $c >/dev/null 2>&1;then
		echo "command $c not found.">&2
		exit
	fi
}
function send_ding(){
	local token=$1
	local contacts=$2
	local context=${3:-"任务已开始提交"}
	(test "x$token" == "x" || test "x$token" == "x#") && return
	(test "x$contacts" == "x" || test "x$contacts" == "x#") && return
	contacts=$(echo "$contacts" | awk -F ',' '{for(i=1;i<=NF;i++){if(a){a=a",\""$i"\""}else{a="\""$i"\""}};print a}')
	local url=$(echo $token | awk '/^https?:\/\//{print $0}')
	local data='{"msgtype":"text","text":{"content":"'$context'"},"at":{"atMobiles":['$contacts'],"isAtAll":false}}'
	check_command curl
	curl -s ${url:-"https://oapi.dingtalk.com/robot/send?access_token=$token"} -H 'Content-Type: application/json' -d "$data"
	echo
}
function check_run(){
    local flag=$1
    check_command yarn
	local appids=($(yarn application -list | awk -v app=$appname '{if($2==app){print $1}}'))
    test ${#appids[@]} -eq 0 && test "x$flag" != "x" && { echo "Spark app $appname already stop.">&2;exit; }
    if test ${#appids[@]} -ne 0;then
  	if [ "x$flag" != "x" ];then
		for flag in ${appids[@]}
		do
			echo "yarn application -kill $flag">&2
			yarn application -kill $flag
		done
		exit;
	fi
        echo "Spark app $appname already running. ${appids[@]}" >&2;
        exit;
    fi
}
function set_abs_lib(){
        local p=$1
        local lp=$2
        test $(echo $p | grep -E '\.properties$' | wc -l) -eq 0 && { echo "$p the file needs to be of type properties">&2;exit; }
        local cap=$(cd $(dirname $p);pwd)
		test "x${!lp:0:1}" != "x/" && eval "$lp=$(get_abs_path $cap/${!lp})"
}
function set_default_lib(){
	local p=$(basename $1)

	if [ $(echo $1 | grep -E "conf/online/$p$" | wc -l) -ne 0 ];then
		echo "../../lib"
	elif [ $(echo $1 | grep -E "conf/$p$" | wc -l) -ne 0 ];then
		echo "../lib"
	else
		echo "lib"
	fi
}

function set_conf_dir(){
	if [ -f "conf/$1" ];then
		echo "conf/$1"
	elif [ -f "conf/online/$1" ];then
		echo "conf/online/$1"
	else
		echo "$1"
	fi
}

function main(){
	local proper=${1:-"$(basename $base).properties"}
	proper=$(set_conf_dir $proper)
	test ! -f "$proper" && { echo "file $proper not found">&2;exit; }
	proper=$(get_abs_path $proper)
	user_proper_file=$proper

	my_include
	test $(check_cmd "java") -eq 0 && check_env "JAVA_HOME"
	test $(check_cmd "spark-submit") -eq 0 && check_env "SPARK_HOME"

	get_param "main" "spark.run.main"
	get_param "main_jar" "spark.run.main.jar"
	get_param "appname" "spark.app.name" "${main}.App"
	get_param "self_param" "spark.run.self.params" "#"
	get_param "lib_path" "spark.run.lib.path" $(set_default_lib $proper)

	get_param "ding_token" "spark.run.alert.ding.api" "#"
	get_param "ding_contacts" "spark.run.alert.ding.contacts" "#"
	get_param "ding_context" "spark.run.alert.ding.context" "任务已经开始提交"

	set_abs_lib "$proper" "lib_path"
	main_jar=$lib_path/$main_jar
	test ! -f "$main_jar" && { echo "$main_jar file does not exist">&2;exit; }
	test "x$self_param" != "x#" && self_params=($(echo $self_param | awk -F ',' '{for(i=1;i<=NF;i++){print $i}}'))

	check_run $2
	set_jars

	local spark_submit=spark-submit
	local main_parameter="--name $appname --properties-file $proper $jars --class $main $main_jar ${self_params[@]}"
	echo "spark-submit $main_parameter"

	test $(check_cmd "spark2-submit") -eq 1 && spark_submit=spark2-submit
	check_command spark-submit
	send_ding "$ding_token" "$ding_contacts" "$appname $ding_context"
	spark-submit $main_parameter
}

test $# -eq 0 && { 
	echo -e "Usage Ex:\n\tbash $base/$0 kafka_2_hdfs.properties">&2;
	exit;
}
main "$@"
