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

package system.packages

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
class MessageHubProduceTests
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
    val messageHubProduce = "messageHubProduce"

    val kafkaUtils = new KafkaUtils

    // these parameter values are 100% valid and should work as-is
    val validParameters = Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "topic" -> topic.toJson,
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "value" -> "Big Trouble is actually a really good Tim Allen movie. Seriously.".toJson)

    behavior of "Message Hub Produce action"

    def testMissingParameter(missingParam : String) = {
        val missingParamsMap = validParameters.filterKeys(_ != missingParam)

        withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", missingParamsMap)) {
            activation =>
                activation.response.success shouldBe false
                activation.response.result.get.toString should include(missingParam)
        }
    }

    it should "Require kafka_brokers_sasl parameter" in {
        testMissingParameter("kafka_brokers_sasl")
    }

    it should "Require user parameter" in {
        testMissingParameter("user")
    }

    it should "Require password parameter" in {
        testMissingParameter("password")
    }

    it should "Require topic parameter" in {
        testMissingParameter("topic")
    }

    it should "Require value parameter" in {
        testMissingParameter("value")
    }

    it should "Reject with bad credentials" in {
        val badAuthParams = validParameters + ("user" -> "ThisWillNeverWork".toJson)

        withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", badAuthParams)) {
            activation =>
                activation.response.success shouldBe false
                activation.response.result.get.toString should include("Authentication failed")
        }
    }

    it should "Reject with bad broker list" in {
        val badBrokerParams = validParameters + ("kafka_brokers_sasl" -> List("localhost:8080").toJson)

        withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", badBrokerParams)) {
            activation =>
                activation.response.success shouldBe false
                activation.response.result.get.toString should include("No brokers available")
        }
    }
}
