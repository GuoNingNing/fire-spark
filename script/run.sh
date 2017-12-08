#!/usr/bin/env bash
base=$(cd $(dirname $0);pwd)

function my_include(){
    local f=${1:-"$HOME/.bash_profile"}
    test -f $f && . $f
}
function check_env(){
        local var=$1
        local default=$2
        local flag=$3
        if [ "x${!var}" == "x" ];then
                if [ "x$default" == "x" ];then
                        echo "ENV variable $var not set." >&2
                        exit;
                else
                        eval "export $var=$default"
                fi
        else
                test "x$default" != "x" && test "x$flag" != "x" && eval "export $var=$default"
        fi
}
function get_param(){
	local var=$1
        local n=$2
        local d=$3

	test ! -f $user_proper_file && { echo "Properties \$user_proper_file file not set">&2;exit; }
        local v=$(grep "^$n=" $user_proper_file | head -1 | awk -F '=' '{s="";for(i=2;i<=NF;i++){if(s){s=s"="$i}else{s=$i}print s}}')
        test "x$v" == "x" && test "x$d" != "x" && v="$d"
        test "x$v" == "x" && { echo "$n not set">&2;exit; }
        eval "$var=$v"
}
function set_jars(){
	local jar=""
	test ! -d "$lib_path" && { echo "lib_path $lib_path not found.">&2;exit; }
	for jar in $(ls $lib_path)
	do
	    if [ "x$jar" != "x$(basename $main_jar)" ]; then
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
function check_run(){
    local flag=${1:-1}
    check_command yarn
    local appids=($(yarn application -list | grep $appname | awk '{print $1}'))
    if test ${#appids[@]} -ne 0;then
  	if [ "x$flag" != "x1" ];then
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
function set_lib_path(){
        local p=$1
        local lp=$2
        test ! -f "$p" && { echo "Config file $p not a document.">&2;exit; }
        test $(echo $p | grep -E '\.properties$' | wc -l) -eq 0 && { echo "$p the file needs to be of type properties">&2;exit; }
        local cap=$(cd $(dirname $p);pwd)
        test "x${!lp:0:1}" != "x/" && eval "$lp=$cap/${!lp}"
}
function check_proper(){
	local p=$1
	test "x$p" == "x" && return
	test ! -f $p && return
	local cap=$(cd $(dirname $p);pwd)
	test "x$cap" == "x/" && cap=""
	local bp=$(basename $p)
	local ps=($(echo "$cap/$bp" | awk -F '/' '{for(i=1;i<=NF;i++){if(!$i){print "#"}else{print $i}}}'))
	case ${ps[${#ps[@]}-2]} in
		"conf")
			echo "${cap%/*}/lib";;
		*)
			echo "$cap/lib";;
	esac
}

function main(){
	local proper=${1:-"$(basename $base).properties"}
	user_proper_file=$proper

	my_include
	check_env "JAVA_HOME" "$HOME/install/java"
	check_env "PATH" '$JAVA_HOME/bin:$PATH' !
	check_env "SPARK_HOME" "$HOME/install/spark-2.2.0-bin-hadoop2.7"
	check_env "PATH" '$SPARK_HOME/bin:$PATH' !

	get_param "main" "spark.run.main"
	get_param "main_jar" "spark.run.main.jar"
	get_param "appname" "spark.app.name" "${main}.App"
	get_param "self_param" "spark.run.self.params" "#"
	get_param "lib_path" "spark.run.lib.path" $(check_proper $proper)
	set_lib_path "$proper" "lib_path"
	main_jar=$lib_path/$main_jar
	test ! -f "$main_jar" && { echo "$main_jar file does not exist">&2;exit; }
	test "x$self_param" != "x#" && self_params=($(echo $self_param | awk -F ',' '{for(i=1;i<=NF;i++){print $i}}'))

	check_run ${2:-"1"}
	set_jars

	echo "spark-submit --name $appname --properties-file $proper $jars --class $main $main_jar" "${self_params[@]}"

	check_command spark-submit
	spark-submit --name $appname --properties-file $proper $jars --class $main $main_jar "${self_params[@]}"
}

test $# -eq 0 && { 
	echo -e "Usage Ex:\n\tbase $base/$0 kafka_2_hdfs.properties">&2;
	exit;
}
main $1 $2
