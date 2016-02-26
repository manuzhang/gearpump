/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.examples.twitter

import java.util.concurrent.LinkedBlockingQueue

import io.gearpump.{TimeStamp, Message}
import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.TaskContext
import twitter4j.StallWarning
import twitter4j.Status
import twitter4j.StatusDeletionNotice
import twitter4j.StatusListener
import twitter4j.TwitterStream
import twitter4j.TwitterStreamFactory
import twitter4j.auth.AccessToken
import twitter4j.conf.ConfigurationBuilder

import scala.collection.mutable.ArrayBuffer

class TwitterSource(
    consumerKey: String,
    consumerSecret: String,
    accessToken: String,
    accessTokenSecret: String) extends DataSource {

  private val queue = new LinkedBlockingQueue[Status]
  private var twitterStream: TwitterStream = null

  /**
   * open connection to data source
   * invoked in onStart() method of [[io.gearpump.streaming.source.DataSourceTask]]
   * @param context is the task context at runtime
   * @param startTime is the start time of system
   */
  override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {
    val listener = new StatusListener() {

      override def onStatus(status: Status) {

        queue.offer(status)
      }

      override def onDeletionNotice(sdn: StatusDeletionNotice) {
      }

      override def onTrackLimitationNotice(i: Int) {
      }

      override def onScrubGeo(l: Long, l1: Long) {
      }

      override def onException(ex: Exception) {
      }

      override def onStallWarning(arg0: StallWarning) {
        // TODO Auto-generated method stub
      }
    }

    twitterStream = new TwitterStreamFactory(
      new ConfigurationBuilder().setJSONStoreEnabled(true).build())
        .getInstance()
    twitterStream.addListener(listener)
    twitterStream.setOAuthConsumer(consumerKey, consumerSecret)
    val token = new AccessToken(accessToken, accessTokenSecret)
    twitterStream.setOAuthAccessToken(token)
    twitterStream.sample()
  }

  /**
   * close connection to data source.
   * invoked in onStop() method of [[io.gearpump.streaming.source.DataSourceTask]]
   */
  override def close(): Unit = {
    twitterStream.shutdown()
  }

  /**
   * read a number of messages from data source.
   * invoked in each onNext() method of [[io.gearpump.streaming.source.DataSourceTask]]
   * @param batchSize max number of messages to read
   * @return a list of messages wrapped in [[io.gearpump.Message]]
   */
  override def read(batchSize: Int): List[Message] = {
    val buffer = ArrayBuffer.empty[Message]
    var count = 0
    while (count < batchSize) {
      Option(queue.poll).foreach(status => buffer += Message(status.toString))
      count += 1
    }
    buffer.toList
  }
}
