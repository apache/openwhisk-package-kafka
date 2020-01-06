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

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import system.utils.KafkaUtils
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.Inside
import org.scalatest.junit.JUnitRunner
import org.apache.kafka.clients.producer.ProducerRecord
import spray.json.DefaultJsonProtocol._
import spray.json._
import common.JsHelpers
import common.TestHelpers
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import ActionHelper._
import common.TestUtils.NOT_FOUND
import org.apache.openwhisk.utils.retry
import org.apache.openwhisk.core.entity.Annotations
import java.util.concurrent.ExecutionException
import common.ActivationResult
import common.TestUtils.SUCCESS_EXIT

@RunWith(classOf[JUnitRunner])
class MessageHubFeedTests
  extends FlatSpec
  with Matchers
  with Inside
  with WskActorSystem
  with BeforeAndAfterAll
  with TestHelpers
  with WskTestHelpers
  with JsHelpers
  with KafkaUtils {

  val topic = "test"
  val sessionTimeout = 10 seconds
  val messagingPackage = "/whisk.system/messaging"
  val messageHubFeed = "messageHubFeed"
  val consumerInitTime = 10000 // ms
  val maxRetries = System.getProperty("max.retries", "60").toInt

  implicit val wskprops = WskProps()
  val wsk = new Wsk()
  val actionName = s"${messagingPackage}/${messageHubFeed}"

  behavior of "Message Hub feed action"

  it should "reject invocation when topic argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a 'topic' parameter.")
    )

    runActionWithExpectedResult(actionName, "dat/missingTopic.json", expectedOutput, false)
  }

  it should "reject invocation when kafka_brokers_sasl argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a 'kafka_brokers_sasl' parameter.")
    )

    runActionWithExpectedResult(actionName, "dat/missingBrokers.json", expectedOutput, false)
  }

  it should "reject invocation when kafka_admin_url argument is missing" in {
    val expectedOutput = JsObject("error" -> JsString(
      "You must supply a 'kafka_admin_url' parameter.")
    )

    runActionWithExpectedResult(actionName, "dat/missingAdminURL.json", expectedOutput, false)
  }

  it should "reject invocation when user argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a 'user' parameter to authenticate with Message Hub.")
    )

    runActionWithExpectedResult(actionName, "dat/missingUser.json", expectedOutput, false)
  }

  it should "reject invocation when password argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a 'password' parameter to authenticate with Message Hub.")
    )

    runActionWithExpectedResult(actionName, "dat/missingPassword.json", expectedOutput, false)
  }

  it should "reject invocation when isJSONData and isBinaryValue are both enable" in {
    val expectedOutput = JsObject(
      "error" -> JsString("isJSONData and isBinaryValue cannot both be enabled."))

    runActionWithExpectedResult(actionName, "dat/multipleValueTypes.json", expectedOutput, false)
  }

  it should "create a trigger, delete that trigger, and quickly create it again with successful trigger fires" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      val ruleName = s"dummyMessageHub-helloKafka-$currentTime"
      val parameters = Map(
        "user" -> getAsJson("user"),
        "password" -> getAsJson("password"),
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> getAsJson("brokers"),
        "topic" -> topic.toJson
      )

      val key = "TheKey"
      val verificationName = s"trigger-$currentTime"
      val defaultAction = Some("dat/createTriggerActions.js")
      val defaultActionName = s"helloKafka-$currentTime"

      createTrigger(assetHelper, triggerName, parameters)

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
      }

      assetHelper.withCleaner(wsk.rule, ruleName) { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      assetHelper.withCleaner(wsk.trigger, verificationName) { (trigger, name) =>
        trigger.get(name, NOT_FOUND)
      }

      produceMessage(topic, key, verificationName)
      retry(wsk.trigger.get(verificationName), 60, Some(1.second))

      wsk.trigger.delete(verificationName, expectedExitCode = SUCCESS_EXIT)
      wsk.trigger.delete(triggerName, expectedExitCode = SUCCESS_EXIT)

      val feedCreationResult = wsk.trigger.create(triggerName, feed = Some(s"/whisk.system/messaging/messageHubFeed"), parameters = parameters)
      val activation = wsk.parseJsonString(feedCreationResult.stdout.substring(0, feedCreationResult.stdout.indexOf("ok: created trigger"))).convertTo[ActivationResult]
      activation.response.success shouldBe true

      wsk.rule.enable(ruleName, expectedExitCode = SUCCESS_EXIT)

      println("Giving the consumer a moment to get ready")
      Thread.sleep(KafkaUtils.consumerInitTime)

      val uuid = activation.response.result.get.fields.get("uuid").get.toString().replaceAll("\"", "")
      consumerExists(uuid)

      produceMessage(topic, key, verificationName)
      retry(wsk.trigger.get(verificationName), 60, Some(1.second))
  }

  it should "fire multiple triggers for two large payloads" in withAssetCleaner(wskprops) {
    // payload size should be under the payload limit, but greater than 50% of the limit
    val testPayloadSize = 600000

    // create trigger
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> getAsJson("user"),
        "password" -> getAsJson("password"),
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> getAsJson("brokers"),
        "topic" -> topic.toJson,
        "isBinaryKey" -> false.toJson,
        "isBinaryValue" -> false.toJson))

      val defaultAction = Some("dat/createTriggerActionsFromKey.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      val verificationName1 = s"trigger1-$currentTime"
      val verificationName2 = s"trigger2-$currentTime"

      assetHelper.withCleaner(wsk.trigger, verificationName1) { (trigger, name) =>
        trigger.get(name, NOT_FOUND)
      }
      assetHelper.withCleaner(wsk.trigger, verificationName2) { (trigger, name) =>
        trigger.get(name, NOT_FOUND)
      }

      // Rapidly produce two messages whose size are each greater than half the allowed payload limit.
      // This should ensure that the feed fires these as two separate triggers.
      println("Rapidly producing two large messages")
      val producer = createProducer()
      val firstMessage = new ProducerRecord(topic, verificationName1, generateMessage(s"first${currentTime}", testPayloadSize))
      val secondMessage = new ProducerRecord(topic, verificationName2, generateMessage(s"second${currentTime}", testPayloadSize))
      producer.send(firstMessage)
      producer.send(secondMessage)
      producer.close()
      retry(wsk.trigger.get(verificationName1), 60, Some(1.second))
      retry(wsk.trigger.get(verificationName2), 60, Some(1.second))
  }

  it should "not fire a trigger for a single oversized message" in withAssetCleaner(wskprops) {
    // payload size should be over the payload limit
    val testPayloadSize = 2000000

    // create trigger
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> getAsJson("user"),
        "password" -> getAsJson("password"),
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> getAsJson("brokers"),
        "topic" -> topic.toJson,
        "isBinaryKey" -> false.toJson,
        "isBinaryValue" -> false.toJson))

      val defaultAction = Some("dat/createTriggerActionsFromKey.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      val verificationName = s"trigger-$currentTime"

      wsk.trigger.get(verificationName, NOT_FOUND)

      // The producer will generate an error as the payload size is too large for the MessageHub brokers
      a[ExecutionException] should be thrownBy produceMessage(topic, verificationName, generateMessage(s"${currentTime}", testPayloadSize))
      a[Exception] should be thrownBy retry(wsk.trigger.get(verificationName), 60, Some(1.second))
  }

  it should "reject trigger update without passing in any updatable parameters" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      val username = getAsJson("user")
      val password = getAsJson("password")
      val admin_url = getAsJson("kafka_admin_url")
      val brokers = getAsJson("brokers")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> username,
        "password" -> password,
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> admin_url,
        "kafka_brokers_sasl" -> brokers,
        "topic" -> topic.toJson,
        "isBinaryKey" -> false.toJson,
        "isBinaryValue" -> false.toJson
      ))

      val run = wsk.action.invoke(actionName, parameters = Map(
        "triggerName" -> triggerName.toJson,
        "lifecycleEvent" -> "UPDATE".toJson,
        "authKey" -> wp.authKey.toJson
      ))

      withActivation(wsk.activation, run) {
        _.response.success shouldBe false
      }
  }

  it should "reject trigger update when both isJSONData and isBinaryValue are enabled" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      val username = getAsJson("user")
      val password = getAsJson("password")
      val admin_url = getAsJson("kafka_admin_url")
      val brokers = getAsJson("brokers")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> username,
        "password" -> password,
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> admin_url,
        "kafka_brokers_sasl" -> brokers,
        "topic" -> topic.toJson,
        "isJSONData" -> true.toJson,
        "isBinaryKey" -> false.toJson,
        "isBinaryValue" -> false.toJson
      ))

      val run = wsk.action.invoke(actionName, parameters = Map(
        "triggerName" -> triggerName.toJson,
        "lifecycleEvent" -> "UPDATE".toJson,
        "authKey" -> wp.authKey.toJson,
        "isBinaryValue" -> true.toJson
      ))

      withActivation(wsk.activation, run) {
        _.response.success shouldBe false
      }
  }

  it should "correctly update isJSONData, isBinaryValue, and isBinaryKey" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      val username = getAsJson("user")
      val password = getAsJson("password")
      val admin_url = getAsJson("kafka_admin_url")
      val brokers = getAsJson("brokers")

      val uuid = createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> username,
        "password" -> password,
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> admin_url,
        "kafka_brokers_sasl" -> brokers,
        "topic" -> topic.toJson,
        "isJSONData" -> true.toJson,
        "isBinaryKey" -> false.toJson,
        "isBinaryValue" -> false.toJson
      ))

      val readRunResult = wsk.action.invoke(actionName, parameters = Map(
        "triggerName" -> triggerName.toJson,
        "lifecycleEvent" -> "READ".toJson,
        "authKey" -> wp.authKey.toJson
      ))

      withActivation(wsk.activation, readRunResult) {
        activation =>
          activation.response.success shouldBe true

          inside (activation.response.result) {
            case Some(result) =>
              val config = result.getFields("config").head.asInstanceOf[JsObject].fields
              config should contain("isBinaryKey" -> false.toJson)
              config should contain("isBinaryValue" -> false.toJson)
              config should contain("isJSONData" -> true.toJson)
          }
      }

      consumerExists(uuid)

      val updateRunResult = wsk.action.invoke(actionName, parameters = Map(
        "triggerName" -> triggerName.toJson,
        "lifecycleEvent" -> "UPDATE".toJson,
        "authKey" -> wp.authKey.toJson,
        "isBinaryValue" -> true.toJson,
        "isBinaryKey" -> true.toJson,
        "isJSONData" -> false.toJson
      ))

      withActivation(wsk.activation, updateRunResult) {
        activation =>
          activation.response.success shouldBe true
      }

      consumerExists(uuid)

      val run = wsk.action.invoke(actionName, parameters = Map(
        "triggerName" -> triggerName.toJson,
        "lifecycleEvent" -> "READ".toJson,
        "authKey" -> wp.authKey.toJson
      ))

      withActivation(wsk.activation, run) {
        activation =>
          activation.response.success shouldBe true

          inside (activation.response.result) {
            case Some(result) =>
              val config = result.getFields("config").head.asInstanceOf[JsObject].fields
              config should contain("isBinaryKey" -> true.toJson)
              config should contain("isBinaryValue" -> true.toJson)
              config should contain("isJSONData" -> false.toJson)
          }
      }

      consumerExists(uuid)
  }

  it should "fire a trigger when a message is posted to message hub before and after update" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val key = "TheKey"
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"

      val uuid = createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> getAsJson("user"),
        "password" -> getAsJson("password"),
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> getAsJson("brokers"),
        "topic" -> topic.toJson
      ))

      val defaultAction1 = Some("dat/createTriggerActions.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction1, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      val verificationName1 = s"trigger1-$currentTime"

      assetHelper.withCleaner(wsk.trigger, verificationName1) { (trigger, name) =>
        trigger.get(name, NOT_FOUND)
      }

      produceMessage(topic, key, verificationName1)
      retry(wsk.trigger.get(verificationName1), 60, Some(1.second))

      println("Updating trigger")

      val updateRunResult = wsk.action.invoke(actionName, parameters = Map(
        "triggerName" -> triggerName.toJson,
        "lifecycleEvent" -> "UPDATE".toJson,
        "authKey" -> wp.authKey.toJson,
        "isBinaryValue" -> true.toJson
      ))

      withActivation(wsk.activation, updateRunResult) {
        _.response.success shouldBe true
      }

      val verificationName2 = s"trigger2-$currentTime"

      assetHelper.withCleaner(wsk.trigger, verificationName2) { (trigger, name) =>
        trigger.get(name, NOT_FOUND)
      }

      val defaultAction2 = Some("dat/createTriggerActionsFromEncodedMessage.js")
      wsk.action.create(defaultActionName, defaultAction2, update = true, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))

      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      consumerExists(uuid)

      produceMessage(topic, key, verificationName2)
      retry(wsk.trigger.get(verificationName2), 60, Some(1.second))
  }

  it should "create a trigger with __bx_creds and fire a trigger when a message is posted to message hub" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val key = "TheKey"
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"

      createTrigger(assetHelper, triggerName, parameters = Map(
        "__bx_creds" -> Map(
          "messagehub" -> Map(
            "user" -> getAsJson("user"),
            "password" -> getAsJson("password"),
            "api_key" -> getAsJson("api_key"),
            "kafka_admin_url" -> getAsJson("kafka_admin_url"),
            "kafka_brokers_sasl" -> getAsJson("brokers"))).toJson,
        "topic" -> topic.toJson
      ))

      val defaultAction1 = Some("dat/createTriggerActions.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction1, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      val verificationName1 = s"trigger1-$currentTime"

      assetHelper.withCleaner(wsk.trigger, verificationName1) { (trigger, name) =>
        trigger.get(name, NOT_FOUND)
      }

      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)
      produceMessage(topic, key, verificationName1)
      retry(wsk.trigger.get(verificationName1), 60, Some(1.second))
  }

  def generateMessage(prefix: String, size: Int): String = {
    val longString = Array.fill[String](size)("0").mkString("")
    s"${prefix}${longString}"
  }
}
