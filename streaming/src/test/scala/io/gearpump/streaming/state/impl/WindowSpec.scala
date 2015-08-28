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
import io.gearpump.streaming.windowing.AlignedWindow
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class WindowSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val windowSizeGen = Gen.chooseNum[Long](1L, 1000L)
  val slidePeriodGen = Gen.chooseNum[Long](1L, 1000L)
  val timestampGen = Gen.chooseNum[Long](0L, 1000L)
  property("Window should only slide when time passes window end") {
    forAll(timestampGen, windowSizeGen, slidePeriodGen) {
      (timestamp: TimeStamp, windowSize: Long, slidePeriod: Long) =>
        val window = new AlignedWindow(windowSize, slidePeriod)
        window.shouldSlide shouldBe false
        window.update(timestamp)
        window.shouldSlide shouldBe timestamp >= windowSize
    }
  }

  property("Window should slide by one or to given timestamp") {
    forAll(timestampGen, windowSizeGen, slidePeriodGen) {
      (timestamp: TimeStamp, windowSize: Long, slidePeriod: Long) =>
        val window = new AlignedWindow(windowSize, slidePeriod)
        window.range shouldBe (0L, windowSize)

        window.slideOneStep()
        window.range shouldBe (slidePeriod, windowSize + slidePeriod)

        window.slideTo(timestamp)
        val (startTime, endTime) = window.range
        if (slidePeriod > windowSize) {
          timestamp should (be >= startTime and be < (startTime + slidePeriod))
        } else {
          timestamp should (be >= startTime and be < endTime)
        }
    }
  }
}
