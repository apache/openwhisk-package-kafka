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
import whisk.utils.retry

@RunWith(classOf[JUnitRunner])
class MessageHubFeedTests
  extends FlatSpec
  with Matchers
  with Inside
  with WskActorSystem
  with BeforeAndAfterAll
  with TestHelpers
  with WskTestHelpers
  with JsHelpers {

  val topic = "test"
  val sessionTimeout = 10 seconds

  val messagingPackage = "/whisk.system/messaging"
  val messageHubFeed = "messageHubFeed"
  val messageHubProduce = "messageHubProduce"

  val consumerInitTime = 10000 // ms

  val kafkaUtils = new KafkaUtils

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

  it should "fire multiple triggers for two large payloads" in withAssetCleaner(wskprops) {
    // payload size should be under the payload limit, but greater than 50% of the limit
    val testPayloadSize = 600000

    // create trigger
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      println(s"Creating trigger ${triggerName}")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "api_key" -> kafkaUtils.getAsJson("api_key"),
        "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "isBinaryKey" -> false.toJson,
        "isBinaryValue" -> false.toJson))

      val defaultAction = Some("dat/createTriggerActionsFromKey.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction)
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

      // It takes a moment for the consumer to fully initialize.
      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      // Rapidly produce two messages whose size are each greater than half the allowed payload limit.
      // This should ensure that the feed fires these as two separate triggers.
      println("Rapidly producing two large messages")
      val producer = kafkaUtils.createProducer()
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
      println(s"Creating trigger ${triggerName}")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "api_key" -> kafkaUtils.getAsJson("api_key"),
        "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "isBinaryKey" -> false.toJson,
        "isBinaryValue" -> false.toJson))

      val defaultAction = Some("dat/createTriggerActionsFromKey.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction)
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      val verificationName = s"trigger-$currentTime"

      wsk.trigger.get(verificationName, NOT_FOUND)

      // It takes a moment for the consumer to fully initialize.
      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      println("Producing an oversized message")
      val producer = kafkaUtils.createProducer()
      val bigMessage = new ProducerRecord(topic, verificationName, generateMessage(s"${currentTime}", testPayloadSize))
      producer.send(bigMessage)
      producer.close()

      a[Exception] should be thrownBy retry(wsk.trigger.get(verificationName), 60, Some(1.second))
  }

  it should "reject trigger update without passing in any updatable parameters" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      println(s"Creating trigger ${triggerName}")

      val username = kafkaUtils.getAsJson("user")
      val password = kafkaUtils.getAsJson("password")
      val admin_url = kafkaUtils.getAsJson("kafka_admin_url")
      val brokers = kafkaUtils.getAsJson("brokers")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> username,
        "password" -> password,
        "api_key" -> kafkaUtils.getAsJson("api_key"),
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
      println(s"Creating trigger $triggerName")

      val username = kafkaUtils.getAsJson("user")
      val password = kafkaUtils.getAsJson("password")
      val admin_url = kafkaUtils.getAsJson("kafka_admin_url")
      val brokers = kafkaUtils.getAsJson("brokers")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> username,
        "password" -> password,
        "api_key" -> kafkaUtils.getAsJson("api_key"),
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
      println(s"Creating trigger $triggerName")

      val username = kafkaUtils.getAsJson("user")
      val password = kafkaUtils.getAsJson("password")
      val admin_url = kafkaUtils.getAsJson("kafka_admin_url")
      val brokers = kafkaUtils.getAsJson("brokers")

      val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
        (trigger, _) =>
          trigger.create(triggerName, feed = Some(actionName), parameters = Map(
            "user" -> username,
            "password" -> password,
            "api_key" -> kafkaUtils.getAsJson("api_key"),
            "kafka_admin_url" -> admin_url,
            "kafka_brokers_sasl" -> brokers,
            "topic" -> topic.toJson,
            "isJSONData" -> true.toJson,
            "isBinaryKey" -> false.toJson,
            "isBinaryValue" -> false.toJson
          ))
      }

      withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
        _.response.success shouldBe true
      }

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
  }

  it should "fire a trigger when a message is posted to message hub before and after update" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val key = "TheKey"
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      println(s"Creating trigger $triggerName")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "api_key" -> kafkaUtils.getAsJson("api_key"),
        "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson
      ))

      val defaultAction1 = Some("dat/createTriggerActions.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction1)
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

      println("Producing a message")
      withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "key" -> key.toJson,
        "value" -> verificationName1.toJson
      ))) {
        _.response.success shouldBe true
      }

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
      wsk.action.create(defaultActionName, defaultAction2, update = true)

      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      println("Producing a message")
      withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "key" -> key.toJson,
        "value" -> verificationName2.toJson
      ))) {
        _.response.success shouldBe true
      }

      retry(wsk.trigger.get(verificationName2), 60, Some(1.second))
  }

  it should "create a trigger with __bx_creds and fire a trigger when a message is posted to message hub" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val key = "TheKey"
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      println(s"Creating trigger $triggerName")

      createTrigger(assetHelper, triggerName, parameters = Map(
        "__bx_creds" -> Map(
          "messagehub" -> Map(
            "user" -> kafkaUtils.getAsJson("user"),
            "password" -> kafkaUtils.getAsJson("password"),
            "api_key" -> kafkaUtils.getAsJson("api_key"),
            "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
            "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"))).toJson,
        "topic" -> topic.toJson
      ))

      val defaultAction1 = Some("dat/createTriggerActions.js")
      val defaultActionName = s"helloKafka-${currentTime}"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction1)
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

      println("Producing a message")
      withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "key" -> key.toJson,
        "value" -> verificationName1.toJson
      ))) {
        _.response.success shouldBe true
      }

      retry(wsk.trigger.get(verificationName1), 60, Some(1.second))
  }

  def createTrigger(assetHelper: AssetCleaner, name: String, parameters: Map[String, spray.json.JsValue]) = {
    val feedCreationResult = assetHelper.withCleaner(wsk.trigger, name) {
      (trigger, _) =>
        trigger.create(name, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = parameters)
    }

    withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
      activation =>
        // should be successful
        activation.response.success shouldBe true
    }
  }

  def generateMessage(prefix: String, size: Int): String = {
    val longString = Array.fill[String](size)("0").mkString("")
    s"${prefix}${longString}"
  }
}
