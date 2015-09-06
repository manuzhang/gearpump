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

package io.gearpump.streaming.dsl.plan

import akka.actor.ActorSystem
import io.gearpump._
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.Constants._
import io.gearpump.streaming.Processor
import io.gearpump.streaming.Processor.DefaultProcessor
import io.gearpump.streaming.dsl.op._
import io.gearpump.streaming.dsl.plan.OpTranslator._
import io.gearpump.streaming.dsl.{TypedDataSink, TypedDataSource}
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}
import io.gearpump.streaming.windowing.{Interval, Window}
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.TraversableOnce
import scala.language.postfixOps

/**
 * Translate a OP to a TaskDescription
 */
class OpTranslator extends java.io.Serializable {
  val LOG: Logger = LogUtil.getLogger(getClass)
  def translate(ops: OpChain)(implicit system: ActorSystem): Processor[_ <: Task] = {
    ops.ops.head match {
      case op: MasterOp =>
        val tail = ops.ops.tail
        val func = toFunction(tail)
        val userConfig = UserConfig.empty.withValue(GEARPUMP_STREAMING_OPERATOR, func)

        op match {
          case DataSourceOp(dataSource, parallelism, description) =>
            Processor[SourceTask[Object, Object]](parallelism,
              description = description + "." + func.description,
              taskConf = userConfig.withValue(GEARPUMP_STREAMING_SOURCE, dataSource))
          case groupby@ GroupByOp(_, parallelism, description) =>
            Processor[GroupByTask[Object, Object, Object]](parallelism,
              description = description + "." + func.description,
              taskConf = userConfig.withValue(GEARPUMP_STREAMING_GROUPBY_FUNCTION, groupby))
          case merge: MergeOp =>
            Processor[TransformTask[Object, Object]](1,
              description = op.description + "." + func.description,
              taskConf = userConfig)
          case ProcessorOp(processor, parallelism, description) =>
            DefaultProcessor(parallelism,
              description = description + "." + func.description,
              taskConf = null, processor)
          case DataSinkOp(dataSink, parallelism, description) =>
            Processor[SinkTask[Object]](parallelism,
              description = func.description,
              taskConf = userConfig.withValue(GEARPUMP_STREAMING_SINK, dataSink))
        }
      case op: SlaveOp[_] =>
        val func = toFunction(ops.ops)
        val userConfig = UserConfig.empty.withValue(GEARPUMP_STREAMING_OPERATOR, func)
        Processor[TransformTask[Object, Object]](1,
          description = func.description,
          taskConf = userConfig)
    }
  }

  private def toFunction(ops: List[Op]): SingleInputFunction[Object, Object] = {
    val func: SingleInputFunction[Object, Object] = new DummyInputFunction[Object]()
    val totalFunction = ops.foldLeft(func) { (fun, op) =>

      val opFunction = op match {
        case flatmap: FlatMapOp[Object, Object] => new FlatMapFunction(flatmap.fun, flatmap.description)
        case reduce: ReduceOp[Object] => new ReduceFunction(reduce.fun, reduce.description)
      }
      fun.andThen(opFunction.asInstanceOf[SingleInputFunction[Object, Object]])
    }
    totalFunction.asInstanceOf[SingleInputFunction[Object, Object]]
  }
}

object OpTranslator {

  trait SingleInputFunction[IN, OUT] extends Serializable {
    def process(value: IN): TraversableOnce[OUT]

    def andThen[OUTER](other: SingleInputFunction[OUT, OUTER]): SingleInputFunction[IN, OUTER] = {
      new AndThen(this, other)
    }

    def description: String
  }

  class DummyInputFunction[T] extends SingleInputFunction[T, T] {
    override def andThen[OUTER](other: SingleInputFunction[T, OUTER]): SingleInputFunction[T, OUTER] = {
      other
    }

    //should never be called
    override def process(value: T) = None

    override def description: String = null
  }

  class AndThen[IN, MIDDLE, OUT](first: SingleInputFunction[IN, MIDDLE], second: SingleInputFunction[MIDDLE, OUT]) extends SingleInputFunction[IN, OUT] {
    override def process(value: IN): TraversableOnce[OUT] = {
      first.process(value).flatMap(second.process(_))
    }

    override def description: String = {
      Option(first.description).flatMap { description =>
        Option(second.description).map(description + "." + _)
      } orNull
    }
  }

  class FlatMapFunction[IN, OUT](fun: IN => TraversableOnce[OUT], descriptionMessage: String) extends SingleInputFunction[IN, OUT] {
    override def process(value: IN): TraversableOnce[OUT] = {
      fun(value)
    }

    override def description: String = {
      this.descriptionMessage
    }
  }

  class ReduceFunction[T](fun: (T, T) => T, descriptionMessage: String) extends SingleInputFunction[T, T] {
    private var state: Any = null

    override def process(value: T): TraversableOnce[T] = {
      if (state == null) {
        state = value
      } else {
        state = fun(state.asInstanceOf[T], value)
      }
      Some(state.asInstanceOf[T])
    }

    override def description: String = descriptionMessage
  }

  class GroupByTask[IN, GROUP, OUT](groupBy: IN => GROUP, taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(userConf.getValue[GroupByOp[IN, GROUP]](GEARPUMP_STREAMING_GROUPBY_FUNCTION)(taskContext.system).get.fun,
        taskContext, userConf)
    }

    private var groups = Map.empty[GROUP, SingleInputFunction[IN, OUT]]

    override def onStart(startTime: StartTime): Unit = {
    }

    override def onNext(msg: Message): Unit = {
      val time = msg.timestamp

      val group = groupBy(msg.msg.asInstanceOf[IN])
      if (!groups.contains(group)) {
        val operator = userConf.getValue[SingleInputFunction[IN, OUT]](GEARPUMP_STREAMING_OPERATOR).get
        groups += group -> operator
      }

      val operator = groups(group)

      operator.process(msg.msg.asInstanceOf[IN]).foreach { msg =>
        taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
      }
    }
  }

  class WindowTask[IN, OUT](window: Window, taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    private var intervals = Map.empty[Interval, SingleInputFunction[IN, OUT]]

    override def onStart(startTime: StartTime): Unit = {
    }

    override def onNext(msg: Message): Unit = {
      val interval = window.getInterval(msg.timestamp)
    }

  }

  class SourceTask[T, OUT](source: TypedDataSource[T], operator: Option[SingleInputFunction[T, OUT]], taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(
        userConf.getValue[TypedDataSource[T]](GEARPUMP_STREAMING_SOURCE)(taskContext.system).get,
        userConf.getValue[SingleInputFunction[T, OUT]](GEARPUMP_STREAMING_OPERATOR)(taskContext.system),
        taskContext, userConf)
    }

    override def onStart(startTime: StartTime): Unit = {
      source.open(taskContext, Some(startTime.startTime))
      self ! Message("start", System.currentTimeMillis())
    }

    override def onNext(msg: Message): Unit = {
      val time = System.currentTimeMillis()
      //Todo: determine the batch size
      source.read(1).foreach(msg => {
        operator match {
          case Some(operator) =>
            operator match {
              case bad: DummyInputFunction[T] =>
                taskContext.output(msg)
              case _ =>
                operator.process(msg.msg.asInstanceOf[T]).foreach(msg => {
                  taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
                })
            }
          case None =>
            taskContext.output(msg)
        }
      })
      self ! Message("next", System.currentTimeMillis())
    }

    override def onStop() = {
      source.close()
    }
  }

  class TransformTask[IN, OUT](operator: Option[SingleInputFunction[IN, OUT]], taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(userConf.getValue[SingleInputFunction[IN, OUT]](GEARPUMP_STREAMING_OPERATOR)(taskContext.system), taskContext, userConf)
    }

    override def onStart(startTime: StartTime): Unit = {
    }

    override def onNext(msg: Message): Unit = {
      val time = msg.timestamp

      operator match {
        case Some(operator) =>
          operator.process(msg.msg.asInstanceOf[IN]).foreach { msg =>
            taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
          }
        case None =>
          taskContext.output(new Message(msg.msg, time))
      }
    }
  }

  class SinkTask[T](dataSink: TypedDataSink[T], taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {
    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(userConf.getValue[TypedDataSink[T]](GEARPUMP_STREAMING_SINK)(taskContext.system).get, taskContext, userConf)
    }

    override def onStart(startTime: StartTime): Unit = {
      dataSink.open(taskContext)
    }

    override def onNext(msg: Message): Unit = {
      dataSink.write(msg)
    }

    override def onStop() = {
      dataSink.close()
    }
  }
}
