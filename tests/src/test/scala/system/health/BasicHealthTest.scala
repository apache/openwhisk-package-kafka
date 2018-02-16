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

package system.health

import java.time.Clock
import java.time.Instant

import system.utils.KafkaUtils

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Inside, Matchers}
import org.scalatest.junit.JUnitRunner
import common.JsHelpers
import common.TestHelpers
import common.TestUtils
import common.TestUtils.DONTCARE_EXIT
import common.TestUtils.NOT_FOUND
import common.TestUtils.SUCCESS_EXIT
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, pimpAny}
import com.jayway.restassured.RestAssured
import whisk.utils.retry;

@RunWith(classOf[JUnitRunner])
class BasicHealthTest
  extends FlatSpec
  with Matchers
  with WskActorSystem
  with BeforeAndAfterAll
  with TestHelpers
  with WskTestHelpers
  with Inside
  with JsHelpers {

  val topic = "test"
  val sessionTimeout = 10 seconds

  implicit val wskprops = WskProps()
  val wsk = new Wsk()

  val messagingPackage = "/whisk.system/messaging"
  val messageHubFeed = "messageHubFeed"
  val messageHubProduce = "messageHubProduce"
  val actionName = s"$messagingPackage/$messageHubFeed"

  val consumerInitTime = 10000 // ms

  val kafkaUtils = new KafkaUtils

  val maxRetries = System.getProperty("max.retries", "60").toInt

  behavior of "Message Hub feed"

  it should "create a new trigger" in withAssetCleaner(wskprops) {
    val triggerName = s"newTrigger-${System.currentTimeMillis}"
    println(s"Creating trigger $triggerName")

    (wp, assetHelper) =>
      val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
        (trigger, _) =>
          trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
            "user" -> kafkaUtils.getAsJson("user"),
            "password" -> kafkaUtils.getAsJson("password"),
            "api_key" -> kafkaUtils.getAsJson("api_key"),
            "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
            "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
            "topic" -> topic.toJson
          ))
      }

      withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
        activation =>
          // should be successful
          activation.response.success shouldBe true
          val uuid = activation.response.result.get.fields.get("uuid").get.toString

          // get /health endpoint and ensure it contains the new uuid
          retry({
            val response = RestAssured.given().get(System.getProperty("health_url"))
            assert(response.statusCode() == 200 && response.asString().contains(uuid))
          }, N = 3, waitBeforeRetry = Some(1.second))
      }
  }

  it should "fire a trigger when a message is posted to message hub" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      println(s"Creating trigger $triggerName")

      val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
        (trigger, _) =>
          trigger.create(triggerName, feed = Some(actionName), parameters = Map(
            "user" -> kafkaUtils.getAsJson("user"),
            "password" -> kafkaUtils.getAsJson("password"),
            "api_key" -> kafkaUtils.getAsJson("api_key"),
            "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
            "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
            "topic" -> topic.toJson
          ))
      }

      withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
        activation =>
          // should be successful
          activation.response.success shouldBe true
      }

      // It takes a moment for the consumer to fully initialize.
      println("Giving the consumer a moment to get ready")
      Thread.sleep(consumerInitTime)

      val defaultAction = Some(TestUtils.getTestActionFilename("hello.js"))
      val defaultActionName = s"helloKafka-$currentTime"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction)
      }
      assetHelper.withCleaner(wsk.rule, "rule") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      retry({
        val start = Instant.now(Clock.systemUTC())
        // key to use for the produced message
        val key = "TheKey"

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

        println("Polling for activations")
        val activations = wsk.activation.pollFor(N = 1, Some(triggerName), retries = maxRetries)
        assert(activations.nonEmpty)
      }, N = 3)
  }

  it should "return correct status and configuration" in withAssetCleaner(wskprops) {
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
            "isBinaryKey" -> false.toJson,
            "isBinaryValue" -> false.toJson
          ))
      }

      withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
        activation =>
          // should be successful
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
              val status = result.getFields("status").head.asInstanceOf[JsObject].fields

              config should contain("kafka_brokers_sasl" -> brokers)
              config should contain("isBinaryKey" -> false.toJson)
              config should contain("isBinaryValue" -> false.toJson)
              config should contain("isJSONData" -> false.toJson)
              config should contain("kafka_admin_url" -> admin_url)
              config should contain("password" -> password)
              config should contain("topic" -> topic.toJson)
              config should contain("user" -> username)
              config("triggerName").convertTo[String].split("/").last should equal (triggerName.split("/").last)
              config should not {
                contain key "authKey"
                contain key "triggerURL"
                contain key "uuid"
                contain key "worker"
              }
              status should contain("active" -> true.toJson)
              status should contain key "dateChanged"
              status should not(contain key "reason")
          }
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
        activation =>
          // should be successful
          activation.response.success shouldBe true
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
}
