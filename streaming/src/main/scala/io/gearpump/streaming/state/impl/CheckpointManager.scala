/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.state.impl

import io.gearpump.TimeStamp
import io.gearpump.streaming.transaction.api.CheckpointStore
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object CheckpointManager {
  private val LOG: Logger = LogUtil.getLogger(classOf[CheckpointManager])
}

class CheckpointManager(checkpointInterval: Long,
    checkpointStore: CheckpointStore) {
  import io.gearpump.streaming.state.impl.CheckpointManager._

  private var maxMessageTime = 0L
  private var checkpointTime = checkpointInterval
  private var lastCheckpointTime = 0L

  def recover(timestamp: TimeStamp): Option[Array[Byte]] = {
    checkpointTime = (timestamp / checkpointInterval + 1) * checkpointInterval
    checkpointStore.recover(timestamp)
  }

  def checkpoint(timestamp: TimeStamp, checkpoint: Array[Byte]): Unit = {
    checkpointStore.persist(timestamp, checkpoint)
    lastCheckpointTime = checkpointTime
  }

  def checkpointAsync(timestamp: TimeStamp, checkpoint: Array[Byte])(callback: => Unit)
      (implicit executionContext: ExecutionContext): Unit = {
    Future {
      checkpointStore.persist(timestamp, checkpoint)
    } onComplete {
      case Success(_) => callback
      case Failure(t) => LOG.error(s"checkpoint failed", t)
    }
  }

  def update(messageTime: TimeStamp): Unit = {
    maxMessageTime = Math.max(maxMessageTime, messageTime)
  }

  def shouldCheckpoint(upstreamMinClock: TimeStamp): Boolean = {
    upstreamMinClock >= checkpointTime && checkpointTime > lastCheckpointTime
  }

  def getCheckpointTime: TimeStamp = checkpointTime

  def updateCheckpointTime(): TimeStamp = {
    if (maxMessageTime >= checkpointTime) {
      checkpointTime += (1 + (maxMessageTime - checkpointTime) / checkpointInterval) * checkpointInterval
    }
    checkpointTime
  }

  def close(): Unit = {
    checkpointStore.close()
  }

  private[impl] def getMaxMessageTime: TimeStamp = maxMessageTime
}
