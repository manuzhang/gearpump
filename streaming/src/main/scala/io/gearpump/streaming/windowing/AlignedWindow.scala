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
package io.gearpump.streaming.windowing

import io.gearpump.TimeStamp

/**
 * used in window applications
 * it keeps the current window and slide ahead when the window expires
 */
class AlignedWindow(val windowSize: Long, val slidePeriod: Long) extends Window {

  private var clock: TimeStamp = 0L
  private var startTime = 0L

  def update(clock: TimeStamp): Unit = {
    this.clock = clock
  }

  def slideOneStep(): Unit = {
    startTime += slidePeriod
  }

  def slideTo(timestamp: TimeStamp): Unit = {
    startTime = timestamp / slidePeriod * slidePeriod
  }

  def shouldSlide: Boolean = {
    clock >= (startTime + windowSize)
  }

  def range: (TimeStamp, TimeStamp) = {
    startTime -> (startTime + windowSize)
  }
  /**
   * each message will update state in corresponding Interval[startTime, endTime),
   * which is decided by the message's timestamp where
   *   startTime = Math.max(startTimeOfCurrentWindow, endTimeOfPreviousWindow)
   *   endTime = Math.min(endTimeOfCurrentWindow, startTimeOfNextWindow)
   */
  def getInterval(timestamp: TimeStamp): Interval = {
    val curStart = timestamp / slidePeriod * slidePeriod
    val prevEnd =
      if (timestamp < windowSize) 0L
      else (timestamp - windowSize) / slidePeriod * slidePeriod + windowSize
    val nextStart = (timestamp / slidePeriod + 1) * slidePeriod
    val curEnd =
      if (timestamp < windowSize) windowSize
      else ((timestamp - windowSize) / slidePeriod + 1) * slidePeriod + windowSize
    val start = Math.max(curStart, prevEnd)
    val end = Math.min(curEnd, nextStart)
    Interval(start, end)
  }

}
