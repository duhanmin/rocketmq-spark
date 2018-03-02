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


import java.{util => ju}

import com.alibaba.rocketmq.client.consumer.{DefaultMQPullConsumer, PullStatus}
import com.alibaba.rocketmq.common.message.{MessageExt, MessageQueue}

/**
  * Consumer of single topic partition, intended for cached reuse.
  */

private[rocketmq]
class CachedMQConsumer private(
                                val groupId: String,
                                val client: DefaultMQPullConsumer,
                                val topic: String,
                                val queueId: Int,
                                val name: String,
                                val optionParams: ju.Map[String, String]) extends Logging {

  private val maxBatchSize = optionParams.getOrDefault(RocketMQConfig.PULL_MAX_BATCH_SIZE, "32").toInt

  private var buffer = ju.Collections.emptyList[MessageExt].iterator

  private var nextOffset = -2L

  private var firstPull = true

//  /**
//    * 判断是否有数据
//    * @param queueOffset
//    * @return
//    */
//  def hasNext(queueOffset: Long): Boolean = {
//    if (firstPull) {
//      logInfo(s"Initial fetch for $groupId $topic $name $queueOffset")
//      poll(queueOffset)
//      firstPull = false
//    }
//
//    if (!buffer.hasNext) {
//      poll(queueOffset)
//    }
//    buffer.hasNext
//  }

  /**
    * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
    * Sequential forward access will use buffers, but random access will be horribly inefficient.
    */
  def get(queueOffset: Long): MessageExt = {

    logDebug(s"Get $groupId $topic  $queueId brokerName $name nextOffset $nextOffset requested")

    if (queueOffset != nextOffset) {
      logInfo(s"Initial fetch for $groupId $topic $name $queueOffset")
      poll(queueOffset)
    }

    if (!buffer.hasNext) {
      poll(queueOffset)
    }

    if (buffer.hasNext) {
      val record = buffer.next
      //      assert(record.getQueueOffset == queueOffset,
      //        s"Got wrong record for $groupId $topic $queueId $name even after seeking to offset $queueOffset")
      nextOffset = queueOffset + 1
      record
    } else {
      null
      //      throw new IllegalStateException(s"Failed to get records for $groupId $topic $queueId $name $queueOffset after polling ")
    }
  }

  private def poll(queueOffset: Long) {
    val subExpression = optionParams.getOrDefault(RocketMQConfig.CONSUMER_TAG, "*")
    var p = client.pull(new MessageQueue(topic, name, queueId), subExpression, queueOffset, maxBatchSize)
    var i = 0
    while (p.getPullStatus == PullStatus.OFFSET_ILLEGAL) {
      // it maybe not get the message, so we will retry
      Thread.sleep(100)
      logError(s"Polled failed for $queueId $name $queueOffset $maxBatchSize ${p.toString} ${p.getPullStatus}")
      i = i + 1
      p = client.pull(new MessageQueue(topic, name, queueId), subExpression, queueOffset, maxBatchSize)
      if (i > 10) {
        throw new IllegalStateException(s"Failed to get records for $groupId $topic $queueId $name $queueOffset after polling," +
          s"due to ${p.toString}")
      }
    }
    if (p.getMsgFoundList != null)
      buffer = p.getMsgFoundList.iterator
  }
}

object CachedMQConsumer extends Logging {

  private case class CacheKey(groupId: String, topic: String, queueId: Int, name: String)

  private var groupIdToClient = Map[String, DefaultMQPullConsumer]()

  // Don't want to depend on guava, don't want a cleanup thread, use a simple LinkedHashMap
  private var cache: ju.LinkedHashMap[CacheKey, CachedMQConsumer] = null

  /** Must be called before get, once per JVM, to configure the cache. Further calls are ignored */
  def init(
            initialCapacity: Int,
            maxCapacity: Int,
            loadFactor: Float): Unit = CachedMQConsumer.synchronized {
    if (null == cache) {
      logInfo(s"Initializing cache $initialCapacity $maxCapacity $loadFactor")
      cache = new ju.LinkedHashMap[CacheKey, CachedMQConsumer](
        initialCapacity, loadFactor, true) {
        override def removeEldestEntry(
                                        entry: ju.Map.Entry[CacheKey, CachedMQConsumer]): Boolean = {
          if (this.size > maxCapacity) {
            true
          } else {
            false
          }
        }
      }
    }
  }

  /**
    * Get a cached consumer for groupId, assigned to topic, queueId and names.
    * If matching consumer doesn't already exist, will be created using optionParams.
    */
  def getOrCreate(
                   groupId: String,
                   topic: String,
                   queueId: Int,
                   name: String,
                   optionParams: ju.Map[String, String]): CachedMQConsumer =
    CachedMQConsumer.synchronized {

      val client = if (!groupIdToClient.contains(groupId)) {
        val client = RocketMqUtils.mkPullConsumerInstance(groupId, optionParams, s"$groupId-executor")
        groupIdToClient += groupId -> client
        client
      } else {
        groupIdToClient(groupId)
      }

      val k = CacheKey(groupId, topic, queueId, name)
      if (cache.containsValue(k)) {
        cache.get(k)
      } else {
        logInfo(s"Cache miss for $k")
        logDebug(cache.keySet.toString)
        val c = new CachedMQConsumer(groupId, client, topic, queueId, name, optionParams)
        cache.put(k, c)
        c
      }
    }

  /**
    * Get a fresh new instance, unassociated with the global cache.
    * Caller is responsible for closing
    */
  def getUncached(
                   groupId: String,
                   topic: String,
                   queueId: Int,
                   name: String,
                   optionParams: ju.Map[String, String]): CachedMQConsumer = {
    val client = RocketMqUtils.mkPullConsumerInstance(groupId, optionParams,
      s"$groupId-executor-$queueId-$name")
    new CachedMQConsumer(groupId, client, topic, queueId, name, optionParams)
  }

  /** remove consumer for given groupId, topic, and queueId, if it exists */
  def remove(groupId: String, topic: String, queueId: Int, name: String): Unit = {
    val k = CacheKey(groupId, topic, queueId, name)
    logInfo(s"Removing $k from cache")
    val v = CachedMQConsumer.synchronized {
      cache.remove(k)
    }
  }
}
