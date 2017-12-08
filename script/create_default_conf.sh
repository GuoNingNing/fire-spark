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
			ph="spark";notes="#\n";k=$1;v=$2;;
		3)
			ph="spark";notes="$1";k=$2;v=$3;;
		4)
			ph=$1;notes="$2";k=$3;v=$4;;
		*)
			echo -ne "#$@\n";return;;
	esac
	echo -e "$(_n $notes)\n${ph}.${k}=${v}\n"
}
function user_run_params(){
	_p "必须设置,执行class的全包名称" "run.main" " "
	_p "必须设置,包含main class的jar包\njar必须包含在lib.path当中" "run.main.jar" " "
	_p "提供给执行class的命令行参数,多个参数之间用逗号隔开,参数中不能包含空格等空白符\nEx:param1,param2,.." \
	"run.self.params" " "
	_p "可以是绝对路径,也可以是相对此配置文件的相对路径\n相对路径会自动补全" "lib.path" "lib"
}
function spark_run_params(){
	_p "执行集群设置,不用设置,一般使用YARN" "master" "yarn"
	_p "YARN部署模式\ndefault=cluster" "submit.deployMode" "cluster"
	_p "spark-streaming每个批次间隔时间\ndefault=300" "slide" "300"

	_p "++++++++++++++++++++++Driver节点相关配置+++++++++++++++++++++++++++"
	_p "Driver节点使用内存大小设置\ndefault=1G" "driver.memory" "1g"
	_p "Driver节点使用的cpu个数设置\ndefault=1" "driver.cores" "1"
	_p "Driver节点构建时spark-jar和user-jar冲突时优先使用用户提供的,这是一个实验性质的参数只对cluster模式有效\ndefault=false" \
	"driver.userClassPathFirst" "false"

	_p "++++++++++++++++++++++Executor节点相关配置+++++++++++++++++++++++++"
	_p "Executor个数设置\ndefault=1" "executor.instances" "1"
	_p "Executor使用cpu个数设置\ndefault=1" "executor.cores" "1"
	_p "Executor使用内存大小设置\ndefault=1G" "executor.memory" "1g"
	_p "同driver节点配置作用相同,但是是针对executor的\ndefault=false" "executor.userClassPathFirst" "true"
}

function create_default(){
	create_notes "\nspark self params\n"
	spark_run_params
	create_notes "\nspark process run.sh\nuser parameters\n"
	user_run_params
	create_notes "\nspark process\nSource config\n"
	_p;_p;_p;
	create_notes "\nspark process\nSink config\n"
	_p;_p;_p;
}

create_default
