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

import java.util.Base64
import java.nio.charset.StandardCharsets

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
    val messageHubFeed = "messageHubFeed"
    val messageHubProduce = "messageHubProduce"

    val consumerInitTime = 10000 // ms

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

    it should "Reject trying to decode a non-base64 key" in {
        val badKeyParams = validParameters + ("key" -> "?".toJson) + ("base64DecodeKey" -> true.toJson)

        withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", badKeyParams)) {
            activation =>
                activation.response.success shouldBe false
                activation.response.result.get.toString should include("key parameter is not Base64 encoded")
        }
    }

    it should "Reject trying to decode a non-base64 value" in {
        val badValueParams = validParameters + ("value" -> "?".toJson) + ("base64DecodeValue" -> true.toJson)

        withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", badValueParams)) {
            activation =>
                activation.response.success shouldBe false
                activation.response.result.get.toString should include("value parameter is not Base64 encoded")
        }
    }

    it should "Post a message with a binary value" in withAssetCleaner(wskprops) {
        // create trigger
        val currentTime = s"${System.currentTimeMillis}"

        (wp, assetHelper) =>
            val triggerName = s"/_/binaryValueTrigger-$currentTime"
            println(s"Creating trigger ${triggerName}")

            val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
                (trigger, _) =>
                    trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
                        "user" -> kafkaUtils.getAsJson("user"),
                        "password" -> kafkaUtils.getAsJson("password"),
                        "api_key" -> kafkaUtils.getAsJson("api_key"),
                        "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
                        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
                        "topic" -> topic.toJson))
            }

            withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
                activation =>
                    // should be successful
                    activation.response.success shouldBe true
            }

            // It takes a moment for the consumer to fully initialize.
            println("Giving the consumer a moment to get ready")
            Thread.sleep(consumerInitTime)

            // produce message
            val decodedMessage = "This will be base64 encoded"
            val encodedMessage = Base64.getEncoder.encodeToString(decodedMessage.getBytes(StandardCharsets.UTF_8))
            val base64ValueParams = validParameters + ("base64DecodeValue" -> true.toJson) + ("value" -> encodedMessage.toJson)

            withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", base64ValueParams, blocking=true)) {
                activation =>
                    activation.response.success shouldBe true
            }

            // verify trigger fired
            println("Polling for activations")
            val activations = wsk.activation.pollFor(N = 1, Some(triggerName), retries = 60)
            assert(activations.length > 0)

            val matchingActivations = for {
                id <- activations
                activation = wsk.activation.waitForActivation(id)
                if (activation.isRight && activation.right.get.fields.get("response").toString.contains(decodedMessage))
            } yield activation.right.get

            assert(matchingActivations.length == 1)

            val activation = matchingActivations.head
            activation.getFieldPath("response", "success") shouldBe Some(true.toJson)

            // assert that there exists a message in the activation which has the expected keys and values
            val messages = KafkaUtils.messagesInActivation(activation, field = "value", value = decodedMessage)
            assert(messages.length == 1)
    }

    it should "Post a message with a binary key" in withAssetCleaner(wskprops) {
        // create trigger
        val currentTime = s"${System.currentTimeMillis}"

        (wp, assetHelper) =>
            val triggerName = s"/_/binaryKeyTrigger-$currentTime"
            println(s"Creating trigger ${triggerName}")

            val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
                (trigger, _) =>
                    trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
                        "user" -> kafkaUtils.getAsJson("user"),
                        "password" -> kafkaUtils.getAsJson("password"),
                        "api_key" -> kafkaUtils.getAsJson("api_key"),
                        "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
                        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
                        "topic" -> topic.toJson))
            }

            withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
                activation =>
                    // should be successful
                    activation.response.success shouldBe true
            }

            // It takes a moment for the consumer to fully initialize.
            println("Giving the consumer a moment to get ready")
            Thread.sleep(consumerInitTime)

            // produce message
            val decodedKey = "This will be base64 encoded"
            val encodedKey = Base64.getEncoder.encodeToString(decodedKey.getBytes(StandardCharsets.UTF_8))
            val base64ValueParams = validParameters + ("base64DecodeKey" -> true.toJson) + ("key" -> encodedKey.toJson)

            withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", base64ValueParams, blocking=true)) {
                activation =>
                    activation.response.success shouldBe true
            }

            // verify trigger fired
            println("Polling for activations")
            val activations = wsk.activation.pollFor(N = 1, Some(triggerName), retries = 60)
            assert(activations.length > 0)

            val matchingActivations = for {
                id <- activations
                activation = wsk.activation.waitForActivation(id)
                if (activation.isRight && activation.right.get.fields.get("response").toString.contains(decodedKey))
            } yield activation.right.get

            assert(matchingActivations.length == 1)

            val activation = matchingActivations.head
            activation.getFieldPath("response", "success") shouldBe Some(true.toJson)

            // assert that there exists a message in the activation which has the expected keys and values
            val messages = KafkaUtils.messagesInActivation(activation, field = "key", value = decodedKey)
            assert(messages.length == 1)
    }
}
