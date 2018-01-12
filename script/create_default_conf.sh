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
			ph=${Lprefix:-"spark"};notes="#\n";k=$1;v=$2;;
		3)
			ph=${Lprefix:-"spark"};notes="$1";k=$2;v=$3;;
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
	_p "spark-streaming每个批次间隔时间\ndefault=300" "batch.duration" "300"
	_p "spark网络序列化方式,默认是JavaSerializer,可针对所有类型但速度较慢\n这里使用推荐的Kryo方式\nkafka-0.10必须使用此方式" \
	"serializer" "org.apache.spark.serializer.KryoSerializer"

	_p "++++++++++++++++++++++Driver节点相关配置+++++++++++++++++++++++++++"
	local Lprefix="spark.driver"
	_p "Driver节点使用内存大小设置\ndefault=1G" "memory" "1g"
	_p "Driver节点使用的cpu个数设置\ndefault=1" "cores" "1"
	_p "Driver节点构建时spark-jar和user-jar冲突时优先使用用户提供的,这是一个实验性质的参数只对cluster模式有效\ndefault=false" \
	"userClassPathFirst" "false"

	_p "++++++++++++++++++++++Executor节点相关配置+++++++++++++++++++++++++"
	Lprefix="spark.executor"
	_p "Executor个数设置\ndefault=1" "instances" "1"
	_p "Executor使用cpu个数设置\ndefault=1" "cores" "1"
	_p "Executor使用内存大小设置\ndefault=1G" "memory" "1g"
	_p "同driver节点配置作用相同,但是是针对executor的\ndefault=false" "userClassPathFirst" "true"
}

function spark_dynamic_params(){
	_p "++++++++++++++++++++++++Executor动态分配相关配置++++++++++++++++++++"
	local Lprefix="spark.shuffle.service"
	_p "Executor动态分配的前置服务\ndefault=false" "enabled" "true"
	_p "服务对应的端口,此端口服务是配置在yarn-site中的,由NodeManager服务加载启动\ndefault=7337" "port" "7337"

	Lprefix="spark.dynamicAllocation"
	_p "配置是否启用资源动态分配,此动态分配是针对executor的,需要yarn集群配置支持动态分配\ndefault=false" \
	"enabled" "true"
	_p "释放空闲的executor的时间\ndefault=60s" "executorIdleTimeout" "60s"
	_p "有缓存的executor空闲释放时间\ndefault=infinity(默认不释放)" "cachedExecutorIdleTimeout" "infinity"
	_p "初始化executor的个数,如果设置spark.executor.instances谁小用谁\ndefault=minExecutors(不设置使用此项配置值)" \
	"initialExecutors" "1"
	_p "executor动态分配可分配最大数量\ndefault=infinity" "maxExecutors" "60"
	_p "executor动态收缩的最小数量\ndefault=0" "minExecutors" "1"
	_p "批次调度延迟多长时间开始增加executor\ndefault=1s" "schedulerBacklogTimeout" "1s"
	_p "同上,但是是针对之后的请求\ndefault=SchedulerBacklogTimeout(不设置使用此项配置值)" \
	"sustainedSchedulerBacklogTimeout" "1s"
}

function create_default(){
	create_notes "\nspark process run.sh\nuser config\n"
	user_run_params
	create_notes "\nspark self config\n"
	spark_run_params
	spark_dynamic_params
	create_notes "\nspark process\nSource config\n"
	_p;_p;_p;
	create_notes "\nspark process\nSink config\n"
	_p;_p;_p;
}

create_default
