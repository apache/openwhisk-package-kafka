/*
 * Copyright 2015-2016 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package system.health

import system.utils.KafkaUtils

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import common.JsHelpers
import common.TestHelpers
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import spray.json.DefaultJsonProtocol._
import spray.json.pimpAny

@RunWith(classOf[JUnitRunner])
class KafkaProduceTests
    extends FlatSpec
    with Matchers
    with WskActorSystem
    with BeforeAndAfterAll
    with TestHelpers
    with WskTestHelpers
    with JsHelpers {

    val topic = "test"
    val sessionTimeout = 10 seconds

    implicit val wskprops = WskProps()
    val wsk = new Wsk()

    val messagingPackage = "/whisk.system/messaging"
    val messageHubProduce = "kafkaProduce"

    val kafkaUtils = new KafkaUtils

    behavior of "Kafka Produce action"

    def testMissingParameter(missingParam : String) = {
        var fullParamsMap = Map(
            "topic" -> topic.toJson,
            "brokers" -> kafkaUtils("brokers").asInstanceOf[List[String]].toJson,
            "value" -> "This will fail".toJson)
        var missingParamsMap = fullParamsMap.filterKeys(_ != missingParam)

        withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", missingParamsMap)) {
            activation =>
                activation.response.success shouldBe false
                activation.response.result.get.toString should include(missingParam)
        }
    }

    it should "Require brokers parameter" in {
        testMissingParameter("brokers")
    }

    it should "Require topic parameter" in {
        testMissingParameter("topic")
    }

    it should "Require value parameter" in {
        testMissingParameter("value")
    }
}
