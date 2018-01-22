/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{ApplicationId, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging

/**
  * Created by cloud on 18/1/17.
  */
private[spark] class YarnApplicationMonitor(
   val appId : ApplicationId,
   val hadoopConf : Configuration,
   val sparkConf : SparkConf) extends Logging{

  def this(appID : ApplicationId, spConf : SparkConf) =
    this(appID, SparkHadoopUtil.get.newConfiguration(spConf), spConf)

  private val yarnClient = YarnClient.createYarnClient()
  private val yarnConf = new YarnConfiguration(hadoopConf)

  def run() = {
    yarnClient.init(yarnConf)
    yarnClient.start()
    val yarnApplicationReport = yarnClient.getApplicationReport(appId)
    val state = yarnApplicationReport.getYarnApplicationState
    if(state != YarnApplicationState.RUNNING) {

    }
  }

}

private[spark] object YarnApplicationMonitor extends Logging{

  def main(args: Array[String]): Unit = {
    System.exit(0)
  }

}
