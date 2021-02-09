/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import common.TestUtils.NOT_FOUND
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers

import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.Base64
import java.nio.charset.StandardCharsets

import org.apache.openwhisk.utils.retry
import org.apache.openwhisk.core.entity.Annotations

@RunWith(classOf[JUnitRunner])
class MessageHubProduceTests
    extends FlatSpec
    with Matchers
    with WskActorSystem
    with BeforeAndAfterAll
    with TestHelpers
    with WskTestHelpers
    with JsHelpers
    with KafkaUtils {

    val topic = "test"
    val sessionTimeout = 10 seconds

    implicit val wskprops = WskProps()
    val wsk = new Wsk()

    val messagingPackage = "/whisk.system/messaging"
    val messageHubFeed = "messageHubFeed"
    val messageHubProduce = "messageHubProduce"
    val consumerInitTime = 10000 // ms
    val maxRetries = System.getProperty("max.retries", "60").toInt

    // these parameter values are 100% valid and should work as-is
    val validParameters = Map(
        "user" -> getAsJson("user"),
        "password" -> getAsJson("password"),
        "topic" -> topic.toJson,
        "kafka_brokers_sasl" -> getAsJson("brokers"),
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
        val currentTime = s"${System.currentTimeMillis}"

        (wp, assetHelper) =>
            val triggerName = s"/_/binaryValueTrigger-$currentTime"

            createTrigger(assetHelper, triggerName, parameters = Map(
                "user" -> getAsJson("user"),
                "password" -> getAsJson("password"),
                "api_key" -> getAsJson("api_key"),
                "kafka_admin_url" -> getAsJson("kafka_admin_url"),
                "kafka_brokers_sasl" -> getAsJson("brokers"),
                "topic" -> topic.toJson))

            val defaultAction = Some("dat/createTriggerActions.js")
            val defaultActionName = s"helloKafka-${currentTime}"

            assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
                action.create(name, defaultAction, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
            }

            assetHelper.withCleaner(wsk.rule, s"dummyMessageHub-helloKafka-$currentTime") { (rule, name) =>
                rule.create(name, trigger = triggerName, action = defaultActionName)
            }

            val verificationName = s"trigger-$currentTime"

            assetHelper.withCleaner(wsk.trigger, verificationName) { (trigger, name) =>
                trigger.get(name, NOT_FOUND)
            }

            // produce message
            val encodedMessage = Base64.getEncoder.encodeToString(verificationName.getBytes(StandardCharsets.UTF_8))
            val base64ValueParams = validParameters + ("base64DecodeValue" -> true.toJson) + ("value" -> encodedMessage.toJson)

            println("Producing a message")
            withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", base64ValueParams)) {
                _.response.success shouldBe true
            }

            retry(wsk.trigger.get(verificationName), 60, Some(1.second))
    }

    it should "Post a message with a binary key" in withAssetCleaner(wskprops) {
        // create trigger
        val currentTime = s"${System.currentTimeMillis}"

        (wp, assetHelper) =>
            val triggerName = s"/_/binaryKeyTrigger-$currentTime"

            createTrigger(assetHelper, triggerName, parameters = Map(
                "user" -> getAsJson("user"),
                "password" -> getAsJson("password"),
                "api_key" -> getAsJson("api_key"),
                "kafka_admin_url" -> getAsJson("kafka_admin_url"),
                "kafka_brokers_sasl" -> getAsJson("brokers"),
                "topic" -> topic.toJson))

            val defaultAction = Some("dat/createTriggerActionsFromKey.js")
            val defaultActionName = s"helloKafka-${currentTime}"

            assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
                action.create(name, defaultAction, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
            }

            assetHelper.withCleaner(wsk.rule, s"dummyMessageHub-helloKafka-$currentTime") { (rule, name) =>
                rule.create(name, trigger = triggerName, action = defaultActionName)
            }

            val verificationName = s"trigger-$currentTime"

            assetHelper.withCleaner(wsk.trigger, verificationName) { (trigger, name) =>
                trigger.get(name, NOT_FOUND)
            }

            // produce message
            val encodedKey = Base64.getEncoder.encodeToString(verificationName.getBytes(StandardCharsets.UTF_8))
            val base64ValueParams = validParameters + ("base64DecodeKey" -> true.toJson) + ("key" -> encodedKey.toJson)

            println("Producing a message")
            withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", base64ValueParams)) {
                _.response.success shouldBe true
            }

            retry(wsk.trigger.get(verificationName), 60, Some(1.second))
    }
}
