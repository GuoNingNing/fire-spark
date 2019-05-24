#!/bin/bash
#
# Copyright (c) 2015 The JobX Project
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -----------------------------------------------------------------------------
#Start Script for the JOBX
# -----------------------------------------------------------------------------
#
# Better OS/400 detection: see Bugzilla 31132

#echo color
WHITE_COLOR="\E[1;37m";
RED_COLOR="\E[1;31m";
BLUE_COLOR='\E[1;34m';
GREEN_COLOR="\E[1;32m";
YELLOW_COLOR="\E[1;33m";
RES="\E[0m";

echo_r () {
    # Color red: Error, Failed
    [[ $# -ne 1 ]] && return 1
    printf "[${BLUE_COLOR}spark${RES}] ${RED_COLOR}$1${RES}\n"
}

echo_g () {
    # Color green: Success
    [[ $# -ne 1 ]] && return 1
    printf "[${BLUE_COLOR}spark${RES}] ${GREEN_COLOR}$1${RES}\n"
}

echo_y () {
    # Color yellow: Warning
    [[ $# -ne 1 ]] && return 1
    printf "[${BLUE_COLOR}spark${RES}] ${YELLOW_COLOR}$1${RES}\n"
}

echo_w () {
    # Color yellow: White
    [[ $# -ne 1 ]] && return 1
    printf "[${BLUE_COLOR}spark${RES}] ${WHITE_COLOR}$1${RES}\n"
}

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false
darwin=false
os400=false
hpux=false
case "`uname`" in
CYGWIN*) cygwin=true;;
Darwin*) darwin=true;;
OS400*) os400=true;;
HP-UX*) hpux=true;;
esac

# resolve links - $0 may be a softlink
PRG="$0"

while [[ -h "$PRG" ]]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

#base variables....
APP_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`
APP_BASE="$APP_HOME"
APP_CONF="$APP_BASE"/conf
APP_LOG="$APP_BASE"/logs
APP_LIB="$APP_BASE"/lib
APP_TEMP="$APP_BASE"/temp
[[ ! -d "$APP_LOG" ]] && mkdir ${APP_LOG} >/dev/null
[[ ! -d "$APP_TEMP" ]] && mkdir ${APP_TEMP} >/dev/null

# For Cygwin, ensure paths are in UNIX format before anything is touched
if ${cygwin}; then
  [[ -n "$APP_HOME" ]] && APP_HOME=`cygpath --unix "$APP_HOME"`
  [[ -n "$APP_BASE" ]] && APP_BASE=`cygpath --unix "$APP_BASE"`
fi

# Ensure that neither APP_HOME nor APP_BASE contains a colon
# as this is used as the separator in the classpath and Java provides no
# mechanism for escaping if the same character appears in the path.
case ${APP_HOME} in
  *:*) echo "Using APP_HOME:   $APP_HOME";
       echo "Unable to start as APP_HOME contains a colon (:) character";
       exit 1;
esac
case ${APP_BASE} in
  *:*) echo "Using APP_BASE:   $APP_BASE";
       echo "Unable to start as APP_BASE contains a colon (:) character";
       exit 1;
esac

# For OS400
if ${os400}; then
  # Set job priority to standard for interactive (interactive - 6) by using
  # the interactive priority - 6, the helper threads that respond to requests
  # will be running at the same priority as interactive jobs.
  COMMAND='chgjob job('${JOBNAME}') runpty(6)'
  system ${COMMAND}
  # Enable multi threading
  export QIBM_MULTI_THREADED=Y
fi

chooseApp(){
read -p "Please select application index to shutdown:
$1
exit
" APP_TARGET
echo "${APP_TARGET}"
}

case "$1" in
    start)
        shift
        ###########################################..spark env start...#####################################################################
        PROPER=""
        if [[ $# -eq 0 ]]; then
          PROPER="application.properties"
          echo_w "not input properties-file,use default application.properties"
        else
          #Solve the path problem, arbitrary path, ignore prefix, only take the content after conf/
          PROPER=$(echo "$1"|awk -F 'conf/' '{print $2}')
        fi
        # spark properties file
        if [[ -f "$APP_CONF/$PROPER" ]] ; then
           APP_PROPER="$APP_CONF/$PROPER"
        else
           echo_r "Usage: properties file:$PROPER not exists!!! ";
           exit 1;
        fi
        #spark app name
        APP_NAME=$(grep 'spark.app.name' ${APP_PROPER} | grep -v '^#' | awk -F'=' '{print $2}')
        # spark main jar...
        MAIN_JAR="${APP_LIB}/$(basename ${APP_BASE}).jar"
        #spark main class
        MAIN=$(grep 'spark.main.class' ${APP_PROPER} | grep -v '^#' | awk -F'=' '{print $2}')
        #spark main parameter..
        MAIN_PARAMS=$(grep 'spark.main.params' ${APP_PROPER} | grep -v '^#' | awk -F'params=' '{print $2}')
        #spark application id file
        APP_PID="$APP_TEMP/${APP_NAME}.pid"
        #spark application lock file
        APP_LOCK="$APP_TEMP/${APP_NAME}.lock"

        shift
        APP_PARAMS=""
        if [[ "x$*" != "x" ]]; then
            APP_PARAMS=$*
        fi

        ### assembly all jars.......
        for JAR in $(ls -1 "${APP_LIB}"); do
            if [[ "x$JAR" != "x$(basename ${MAIN_JAR})" ]]; then
                JARS=${APP_LIB}/${JAR},${JARS}
            fi
        done

        [[ "x$JARS" == "x" ]] && { echo_r "Usage: ${APP_LIB} assembly jar error!!! "; exit 1; }

        ##check lock.....
        if [[ -f "${APP_LOCK}" ]] ; then
            echo_r "this app already running.please check it,you cat delete $APP_LOCK by yourself...";
            exit 1
        else
            #create lock file
            touch ${APP_LOCK}
        fi
        ###########################################..spark env end...#####################################################################

        EXIT_CODE=0

        sudo -u hdfs yarn application --list | grep -i "spark" | grep "${APP_NAME}" >/dev/null

        if [[ $? -eq 0 ]]; then
            echo_r "${APP_NAME} already exists!!!"
            EXIT_CODE=1
        else
            echo_g "${APP_NAME} Starting..."
            APP_LOG_DATE=`date "+%Y%m%d_%H%M%S"`
            APP_OUT="${APP_LOG}/${APP_NAME}-${APP_LOG_DATE}.log"

            sudo -u hdfs spark2-submit \
                --name ${APP_NAME} \
                --queue spark \
                --properties-file ${APP_PROPER} \
                --jars ${JARS} ${APP_PARAMS} \
                --class ${MAIN} \
                ${MAIN_JAR} ${MAIN_PARAMS} \
                >> ${APP_OUT} 2>&1

            EXIT_CODE=$?

            if [[ ${EXIT_CODE} -eq 0 ]] ; then
                 #get application_id
                 PID="application_`grep "tracking URL:" ${APP_OUT}|awk -F'/application_' '{print $2}'|awk -F'/' '{print $1}'`"
                 #write application_id to ${APP_PID}
                 echo ${PID} > ${APP_PID}
                 echo_g "${APP_NAME} start successful,application_id:${PID}"
            else
                echo_r "${APP_NAME} start error,please log:${APP_OUT}"
            fi
        fi
         #start done,and delete lock file.....
        [[ -f "${APP_LOCK}" ]] && rm -rf ${APP_LOCK}
        exit ${EXIT_CODE}
        ;;
    stop)
      PID_LEN="`ls ${APP_TEMP}/ | grep ".pid"|wc -l`"
      if [[ x"${PID_LEN}" == x"0" ]] ; then
           echo_r "cat not found application_id!!!"
      else
        #only one pid...
        if [[ x"${PID_LEN}" == x"1" ]] ; then
           PID_FILE="`ls ${APP_TEMP}/ | grep ".pid"`"
           PID="`cat ${APP_TEMP}/${PID_FILE}`"
        else
           ###more pid file.....
           index=0
           for PID_FILE in `ls ${APP_TEMP}/ | grep ".pid"`; do
               PID_FILES[index]=${PID_FILE}
               index=`expr ${index} + 1`
           done
           index=0
           APPS=$(for PID_FILE in `ls ${APP_TEMP}/ | grep ".pid"`; do
               PID_APP=$(echo "$PID_FILE"|awk -F'.pid' '{print $1}')
               echo "$index) ${PID_APP}"
               index=`expr ${index} + 1`
           done)

           APP_TARGET=$(chooseApp "${APPS}")

           if [[ x"${APP_TARGET}" == x"" ]] ; then
               echo_w "Usage error."
               exit 1
           elif [[ x"${APP_TARGET}" == x"exit" ]] ; then
               echo_w "exit shutdown."
               exit 0
           elif [[ -n "`echo ${APP_TARGET} | sed 's/[0-9]//g'`" ]] ; then
              echo_r "Usage error."
              exit 1;
           else
               PID_FILE=${PID_FILES[${APP_TARGET}]}
               PID=`cat ${APP_TEMP}/${PID_FILE}`
           fi
        fi

        sudo -u hdfs yarn application -kill ${PID}  >/dev/null
        if [[ $? -eq 0 ]] ; then
           echo_g "stop successful,application_id:${PID}"
           [[ -f "${APP_TEMP}/${PID_FILE}" ]] && rm -rf ${APP_TEMP}/${PID_FILE}
        else
           echo_r "stop error,application_id:${PID}"
        fi

      fi
      exit $?
      ;;
    *)
      echo_g "Unknown command: $1"
      echo_g "commands:"
      echo_g "  start             Start"
      echo_g "  stop              Stop"
      echo_g "                    are you running?"
      exit 1
    ;;
esac
