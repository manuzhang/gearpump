/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding cstateyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a cstatey of the License at
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
import io.gearpump.streaming.state.api.{Group, MonoidState, Serializer}
import io.gearpump.streaming.state.impl.WindowState._
import io.gearpump.streaming.task.TaskContext
import io.gearpump.streaming.windowing.{CheckpointAlignedWindow, Interval}
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.TreeMap



object WindowState {
  val WINDOW = "window"

  val LOG: Logger = LogUtil.getLogger(classOf[WindowState[_]])
}

/**
 * this is a list of states, each of which is bounded by a time window
 * state of each window doesn't affect each other
 *
 * WindowState requires a Algebird Group to be passed in
 * Group augments Monoid with a minus function which makes it
 * possible to undo the update by messages that have left the window
 */
class WindowState[T](group: Group[T],
                     serializer: Serializer[TreeMap[Interval, T]],
                     taskContext: TaskContext,
                     window: CheckpointAlignedWindow)
  extends MonoidState[T](group) {
  /**
   * each interval has a state updated by message with timestamp in
   * [interval.startTime, interval.endTime)
   */
  private var intervalStates = TreeMap.empty[Interval, T]

  private var lastCheckpointTime = 0L

  override def recover(timestamp: TimeStamp, bytes: Array[Byte]): Unit = {
    window.slideTo(timestamp)
    serializer.deserialize(bytes)
      .foreach { states =>
      intervalStates = states
      left = states.foldLeft(left) { case (accum, iter) =>
        group.plus(accum, iter._2)
      }
    }
  }

  override def update(timestamp: TimeStamp, t: T): Unit = {
    val (startTime, endTime) = window.range
    if (timestamp >= startTime && timestamp < endTime) {
      updateState(timestamp, t)
    }

    updateIntervalStates(timestamp, t, checkpointTime)

    val upstreamMinClock = taskContext.upstreamMinClock
    window.update(upstreamMinClock)

    if (window.shouldSlide) {
      window.slideOneStep()

      val (newStartTime, newEndTime) = window.range
      getIntervalStates(startTime, newStartTime).foreach { case (_, st) =>
        left = group.minus(left, st)
      }
      if (checkpointTime > endTime) {
        getIntervalStates(endTime, checkpointTime).foreach { case (_, st) =>
          left = group.plus(left, st)
        }
      } else {
        getIntervalStates(endTime, newEndTime).foreach { case (_, st) =>
          right = group.plus(right, st)
        }
      }
    }
  }

  override def checkpoint(): Array[Byte] = {
    left = group.plus(left, right)
    right = group.zero

    val states = getIntervalStates(window.range._1, checkpointTime)
    lastCheckpointTime = checkpointTime
    LOG.debug(s"checkpoint time: $checkpointTime; checkpoint value: ($checkpointTime, $states)")
    serializer.serialize(states)
  }


  private[impl] def updateIntervalStates(timestamp: TimeStamp, t: T, checkpointTime: TimeStamp): Unit = {
    val interval = window.getInterval(timestamp, lastCheckpointTime, checkpointTime)
    intervalStates.get(interval) match {
      case Some(st) =>
        intervalStates += interval -> group.plus(st, t)
      case None =>
        intervalStates += interval -> group.plus(group.zero, t)
    }
  }

  private[impl] def getIntervalStates(startTime: TimeStamp, endTime: TimeStamp): TreeMap[Interval, T] = {
    intervalStates.dropWhile(_._1.endTime <= startTime).takeWhile(_._1.endTime <= endTime)
  }

}
