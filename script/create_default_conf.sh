#!/usr/bin/env bash
test -f /etc/profile && . /etc/profile
test -f $HOME/.bash_profile && . $HOME/.bash_profile
base=$(cd $(dirname $0);pwd)

function create_notes(){
	local msg=$1
	local col=${2:-55}
	awk -vc=$col 'function as(ll,is,ls){for(i=1;i<ll;i++){ls=ls""is};return ls};
	BEGIN{print as(c,"#")}'
	echo -e "$msg" | awk -vc=$col 'function as(ll,is,ls){for(i=1;i<ll;i++){ls=ls""is};return ls};
	{m=$0;l=(c-length(m)-2)/2;s=as(l-1," ");s1=as(c-length("#"s""m)-1," ");print "#"s""m""s1"#"}'
	awk -vc=$col -vr=$row 'function as(ll,is,ls){for(i=1;i<ll;i++){ls=ls""is};return ls};
	BEGIN{print as(c,"#")}'
}
function _n(){
	echo -e "$@" | awk '{print "#"$0}'
}
function _p(){
	local ph notes k v
	case $# in
		2)
			ph="spark.run";notes="#\n";k=$1;v=$2;;
		3)
			ph="spark.run";notes="$1";k=$2;v=$3;;
		4)
			ph=$1;notes="$2";k=$3;v=$4;;
		*)
			echo -e "#\n";return;;
	esac
	echo -e "$(_n $notes)\n${ph}.${k}=${v}\n"
}
function standard_run_param(){
	_p "必须设置,执行class的全包名称" "main" "z.cloud.t1.Tz"
	_p "必须设置,包含main class的jar包\njar必须包含在lib.path当中" "main.jar" "t1-1.0-SNAPSHOT.jar"
	_p "default=1" "executor.num" "1"
	_p "default=1" "executor.core" "1"
	_p "default=1G" "executor.mem" "1G"
	_p "default=1G" "driver.mem" "1G"
	_p "default=cluster" "deploy.mode" "cluster"
	_p "default=param1,param2,..." "self.params" " "
	_p "可以是绝对路径,也可以是相对此配置文件的相对路径\n相对路径会自动补全" "lib.path" "lib"
}

function create_default(){
	create_notes "\nspark self params\n"
	_p "spark" "default=300" "slide" "300"
	create_notes "\nspark process run.sh\nparameters\n"
	standard_run_param
	create_notes "\nspark process\nSource config\n"
	_p;_p;_p;
	create_notes "\nspark process\nSink config\n"
	_p;_p;_p;
}

create_default
