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
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.Inside
import org.scalatest.junit.JUnitRunner
import spray.json.DefaultJsonProtocol._
import spray.json._
import common.JsHelpers
import common.TestUtils
import common.TestHelpers
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import ActionHelper._
import java.util.Base64
import java.nio.charset.StandardCharsets
import java.time.{Clock, Instant}


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

  implicit val wskprops = WskProps()
  val wsk = new Wsk()
  val actionName = s"${messagingPackage}/${messageHubFeed}"

  val defaultAction = Some(TestUtils.getTestActionFilename("hello.js"))
  val defaultActionName = "hello"

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

  it should "fire a trigger when a binary message is posted to message hub" in withAssetCleaner(wskprops) {
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
        "isBinaryKey" -> true.toJson,
        "isBinaryValue" -> true.toJson))

      assetHelper.withCleaner(wsk.action, "hello") { (action, name) =>
        action.create(name, defaultAction)
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      // It takes a moment for the consumer to fully initialize.
      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      // key to use for the produced message
      val key = "TheKey"
      val encodedCurrentTime = Base64.getEncoder.encodeToString(currentTime.getBytes(StandardCharsets.UTF_8))
      val encodedKey = Base64.getEncoder.encodeToString(key.getBytes(StandardCharsets.UTF_8))

      withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "key" -> key.toJson,
        "value" -> currentTime.toJson))) {
        _.response.success shouldBe true
      }

      println("Polling for activations")
      val activations = wsk.activation.pollFor(N = 100, Some(triggerName), retries = 60)
      assert(activations.length > 0)

      val matchingActivations = for {
        id <- activations
        activation = wsk.activation.waitForActivation(id)
        if (activation.isRight && activation.right.get.fields.get("response").toString.contains(encodedCurrentTime))
      } yield activation.right.get

      assert(matchingActivations.length == 1)

      val activation = matchingActivations.head
      activation.getFieldPath("response", "success") shouldBe Some(true.toJson)

      // assert that there exists a message in the activation which has the expected keys and values
      val messages = KafkaUtils.messagesInActivation(activation, field = "value", value = encodedCurrentTime)
      assert(messages.length == 1)

      val message = messages.head
      message.getFieldPath("topic") shouldBe Some(topic.toJson)
      message.getFieldPath("key") shouldBe Some(encodedKey.toJson)
  }

  it should "not fire a single trigger with an oversized payload" in withAssetCleaner(wskprops) {
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

      assetHelper.withCleaner(wsk.action, "hello") { (action, name) =>
        action.create(name, defaultAction)
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      // It takes a moment for the consumer to fully initialize.
      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      // Rapidly produce two messages whose size are each greater than half the allowed payload limit.
      // This should ensure that the feed fires these as two separate triggers.
      println("Rapidly producing two large messages")
      val producer = kafkaUtils.createProducer()
      val firstMessage = new ProducerRecord(topic, "key", generateMessage(s"first${currentTime}", testPayloadSize))
      val secondMessage = new ProducerRecord(topic, "key", generateMessage(s"second${currentTime}", testPayloadSize))
      producer.send(firstMessage)
      producer.send(secondMessage)
      producer.close()

      // verify there are two trigger activations required to handle these messages
      println("Polling for activations")
      val activations = wsk.activation.pollFor(N = 100, Some(triggerName), retries = 60)

      println("Verifying activation content")
      val matchingActivations = for {
        id <- activations
        activation = wsk.activation.waitForActivation(id)
        if (activation.isRight && (activation.right.get.fields.get("response").toString.contains(s"first${currentTime}") ||
          activation.right.get.fields.get("response").toString.contains(s"second${currentTime}")))
      } yield activation.right.get

      assert(matchingActivations.length == 2)
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

      assetHelper.withCleaner(wsk.action, "hello") { (action, name) =>
        action.create(name, defaultAction)
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      // It takes a moment for the consumer to fully initialize.
      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      println("Producing an oversized message")
      val producer = kafkaUtils.createProducer()
      val bigMessage = new ProducerRecord(topic, "key", generateMessage(s"${currentTime}", testPayloadSize))
      producer.send(bigMessage)
      producer.close()

      // verify there are no activations that match
      println("Polling for activations")
      val activations = wsk.activation.pollFor(N = 100, Some(triggerName), retries = 60)

      println("Verifying activation content")
      val matchingActivations = for {
        id <- activations
        activation = wsk.activation.waitForActivation(id)
        if (activation.isRight && (activation.right.get.fields.get("response").toString.contains(s"first${currentTime}")))
      } yield activation.right.get

      assert(matchingActivations.length == 0)
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
        activation =>
          activation.response.success shouldBe false
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
        activation =>
          activation.response.success shouldBe false
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

      assetHelper.withCleaner(wsk.action, "hello") { (action, name) =>
        action.create(name, defaultAction)
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      val first = Instant.now(Clock.systemUTC())

      println("Producing a message")
      withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "key" -> key.toJson,
        "value" -> currentTime.toJson
      ))) {
        _.response.success shouldBe true
      }

      checkForActivations(triggerName, first, topic, key, currentTime)

      println("Updating trigger")

      val updateRunResult = wsk.action.invoke(actionName, parameters = Map(
        "triggerName" -> triggerName.toJson,
        "lifecycleEvent" -> "UPDATE".toJson,
        "authKey" -> wp.authKey.toJson,
        "isBinaryValue" -> true.toJson
      ))

      withActivation(wsk.activation, updateRunResult) {
        activation =>
          activation.response.success shouldBe true
      }

      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      val second = Instant.now(Clock.systemUTC())
      val encodedCurrentTime = Base64.getEncoder.encodeToString(currentTime.getBytes(StandardCharsets.UTF_8))

      println("Producing a message")
      withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", Map(
        "user" -> kafkaUtils.getAsJson("user"),
        "password" -> kafkaUtils.getAsJson("password"),
        "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
        "topic" -> topic.toJson,
        "key" -> key.toJson,
        "value" -> currentTime.toJson
      ))) {
        _.response.success shouldBe true
      }

      checkForActivations(triggerName, second, topic, key, encodedCurrentTime)
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

  def checkForActivations(triggerName: String, since: Instant, topic: String, key: String, value: String) = {
    println("Polling for activations")
    val activations = wsk.activation.pollFor(N = 100, Some(triggerName), since = Some(since), retries = 30)
    assert(activations.length > 0)

    println("Validating content of activation(s)")
    val matchingActivations = for {
      id <- activations
      activation = wsk.activation.waitForActivation(id)
      if (activation.isRight && activation.right.get.fields.get("response").toString.contains(value))
    } yield activation.right.get

    assert(matchingActivations.length == 1)

    val activation = matchingActivations.head
    activation.getFieldPath("response", "success") shouldBe Some(true.toJson)

    // assert that there exists a message in the activation which has the expected keys and values
    val messages = KafkaUtils.messagesInActivation(activation, field = "value", value = value)
    assert(messages.length == 1)

    val message = messages.head
    message.getFieldPath("topic") shouldBe Some(topic.toJson)
    message.getFieldPath("key") shouldBe Some(key.toJson)
  }

  def generateMessage(prefix: String, size: Int): String = {
    val longString = Array.fill[String](size)("0").mkString("")
    s"${prefix}${longString}"
  }
}
