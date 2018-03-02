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

package org.apache.rocketmq.spark

import com.alibaba.rocketmq.common.message.MessageQueue
import org.apache.spark.Partition


/**
  * the Partition for RocketMqRDD
  * @param index the partition id for rdd
  * @param topic the rockermq topic
  * @param name the broker name
  * @param queueId the rocketmq queue id
  * @param fromOffset inclusive starting offset
  * @param untilOffset exclusive ending offset
  */
class RocketMqRDDPartition(
                         val index: Int,
                         val topic: String,
                         val name:String,
                         val queueId: Int,
                         val fromOffset: Long,
                         val untilOffset: Long
                       ) extends Partition {
  /** Number of messages this partition refers to */
  def count(): Long = untilOffset - fromOffset


//  /** rocketmq TopicQueueId object, for convenience */
//  def topicQueueId(): TopicQueueId = new TopicQueueId(topic, queueId)

  def messageQueue():MessageQueue = new MessageQueue(topic,name,queueId)

//  def brokerName(): Set[String] = {
//    offsetRange.map(_.brokerName).sorted.toSet
//  }


  override def toString: String = {
    s"$index $topic $name $queueId $fromOffset $untilOffset"
  }
}