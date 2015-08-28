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

package io.gearpump.streaming.windowing

import io.gearpump._

class CheckpointAlignedWindow(windowSize: Long, slidePeriod: Long) extends AlignedWindow(windowSize, slidePeriod) {
  val lastCheckpointTime: TimeStamp = ???
  val checkpointTime: TimeStamp = ???

  override def getInterval(timestamp: TimeStamp): Interval = {
    val windowSize = windowSize
    val slidePeriod = slidePeriod
    val lowerBound1 = timestamp / slidePeriod * slidePeriod
    val lowerBound2 =
      if (timestamp < windowSize) 0L
      else (timestamp - windowSize) / slidePeriod * slidePeriod + windowSize
    val upperBound1 = (timestamp / slidePeriod + 1) * slidePeriod
    val upperBound2 =
      if (timestamp < windowSize) windowSize
      else ((timestamp - windowSize) / slidePeriod + 1) * slidePeriod + windowSize
    val lowerBound = Math.max(lowerBound1, lowerBound2)
    val upperBound = Math.min(upperBound1, upperBound2)
    if (checkpointTime > timestamp) {
      Interval(Math.max(lowerBound, lastCheckpointTime), Math.min(upperBound, checkpointTime))
    } else {
      Interval(Math.max(lowerBound, checkpointTime), upperBound)
    }
  }
}

