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

import akka.actor.ActorSystem
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import io.gearpump.partitioner.HashPartitioner
import io.gearpump.streaming.source.DataSourceProcessor
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.util.{Graph, AkkaApp}
import io.gearpump.util.Graph._

object PrintTwitterStream extends AkkaApp with ArgumentsParser {
  private val CONSUMER_KEY = "consumerKey"
  private val CONSUMER_SECRET = "consumerSecret"
  private val ACCESS_TOKEN = "accessToken"
  private val ACCESS_TOKEN_SECRET = "accessTokenSecret"
  private val SOURCE_TASK = "sourceTask"
  private val PRINT_TASK = "printTask"

  override val options: Array[(String, CLIOption[Any])] = Array(
    CONSUMER_KEY -> CLIOption[String]("<twitter consumer key>", required = true),
    CONSUMER_SECRET -> CLIOption[String]("<twitter consumer secret>", required = true),
    ACCESS_TOKEN -> CLIOption[String]("<twitter access token>", required = true),
    ACCESS_TOKEN_SECRET -> CLIOption[String]("<twitter access token secret>", required = true),
    SOURCE_TASK -> CLIOption[Int]("<how many source tasks>", required = false, defaultValue = Some(1)),
    PRINT_TASK -> CLIOption[Int]("<how many print tasks>", required = false, defaultValue = Some(1))
  )

  def application(config: ParseResult, system: ActorSystem) : StreamApplication = {
    implicit val actorSystem = system

    val consumerKey = config.getString(CONSUMER_KEY)
    val consumerSecret = config.getString(CONSUMER_SECRET)
    val accessToken = config.getString(ACCESS_TOKEN)
    val accessTokenSecret = config.getString(ACCESS_TOKEN_SECRET)
    val sourceTaskNum = config.getInt(SOURCE_TASK)
    val printTaskNum = config.getInt(PRINT_TASK)

    val source = new TwitterSource(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    val sourceProcessor = DataSourceProcessor(source, sourceTaskNum)
    val printProcessor = Processor[PrintTask](printTaskNum)
    val partitioner = new HashPartitioner

    val app = StreamApplication("PrintTwitterStream", Graph(sourceProcessor ~ partitioner ~> printProcessor), UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)

    val context = ClientContext(akkaConf)
    val app = application(config, context.system)
    context.submit(app)
    context.close()

  }
}
