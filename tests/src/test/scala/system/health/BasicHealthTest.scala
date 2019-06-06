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

import common.TestUtils.NOT_FOUND
import common._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Inside, Matchers}
import spray.json.DefaultJsonProtocol._
import spray.json._
import system.utils.KafkaUtils
import org.apache.openwhisk.utils.retry
import org.apache.openwhisk.core.entity.Annotations

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class BasicHealthTest
  extends FlatSpec
  with Matchers
  with WskActorSystem
  with BeforeAndAfterAll
  with TestHelpers
  with WskTestHelpers
  with Inside
  with JsHelpers
  with KafkaUtils {

  val topic = "test"
  val sessionTimeout = 10 seconds

  implicit val wskprops = WskProps()
  val wsk = new Wsk()

  val messagingPackage = "/whisk.system/messaging"
  val messageHubFeed = "messageHubFeed"
  val messageHubProduce = "messageHubProduce"
  val actionName = s"$messagingPackage/$messageHubFeed"
  val maxRetries = System.getProperty("max.retries", "60").toInt

  behavior of "Message Hub feed"

  it should "create a consumer and fire a trigger when a message is posted to messagehub" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"

      createTrigger(assetHelper, triggerName, parameters = Map(
        "user" -> getAsJson("user"),
        "password" -> getAsJson("password"),
        "api_key" -> getAsJson("api_key"),
        "kafka_admin_url" -> getAsJson("kafka_admin_url"),
        "kafka_brokers_sasl" -> getAsJson("brokers"),
        "topic" -> topic.toJson
      ))

      // This action creates a trigger if it gets executed.
      // The name of the trigger will be the message, that has been send to kafka.
      // We only create this trigger to verify, that the action has been executed after sending the message to kafka.
      val defaultAction = Some("dat/createTriggerActions.js")
      val defaultActionName = s"helloKafka-$currentTime"

      assetHelper.withCleaner(wsk.action, defaultActionName) { (action, name) =>
        action.create(name, defaultAction, annotations = Map(Annotations.ProvideApiKeyAnnotationName -> JsBoolean(true)))
      }

      assetHelper.withCleaner(wsk.rule, s"dummyMessageHub-helloKafka-$currentTime") { (rule, name) =>
        rule.create(name, trigger = triggerName, action = defaultActionName)
      }

      // key to use for the produced message
      val key = "TheKey"

      val verificationName = s"trigger-$currentTime"

      // Check that the verification trigger does not exist before the action ran.
      // This will also clean up the trigger after the test.
      assetHelper.withCleaner(wsk.trigger, verificationName) { (trigger, name) =>
        trigger.get(name, NOT_FOUND)
      }

      produceMessage(topic, key, verificationName)

      // Check if the trigger, that should have been created as reaction on the kafka-message, has been created.
      // The trigger should have been created by the action, that has been triggered by the kafka message.
      // If we cannot find it, the most probably the action did not run.
      retry(wsk.trigger.get(verificationName), 60, Some(1.second))
  }

  it should "return correct status and configuration" in withAssetCleaner(wskprops) {
    val currentTime = s"${System.currentTimeMillis}"

    (wp, assetHelper) =>
      val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
      println(s"Creating trigger $triggerName")

      val username = getAsJson("user")
      val password = getAsJson("password")
      val admin_url = getAsJson("kafka_admin_url")
      val brokers = getAsJson("brokers")

      val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
        (trigger, _) =>
          trigger.create(triggerName, feed = Some(actionName), parameters = Map(
            "user" -> username,
            "password" -> password,
            "api_key" -> getAsJson("api_key"),
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
}
