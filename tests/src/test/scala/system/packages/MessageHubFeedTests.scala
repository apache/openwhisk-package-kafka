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

import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import spray.json.JsString
import spray.json.pimpAny

import common.JsHelpers
import common.TestHelpers
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import ActionHelper._

import java.util.Base64
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class MessageHubFeedTests
  extends FlatSpec
    with Matchers
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
  val actionName = "messageHubFeedAction"
  val actionFile = "../action/messageHubFeed.js"

  behavior of "Message Hub feed action"

  override def beforeAll() {
    wsk.action.create(actionName, Some(actionFile))
    super.beforeAll()
  }

  override def afterAll()  {
    wsk.action.delete(actionName)
    super.afterAll()
  }

  it should "reject invocation when topic argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a \"topic\" parameter.")
    )

    runActionWithExpectedResult(actionName, "dat/missingTopic.json", expectedOutput, false)
  }

  it should "reject invocation when kafka_brokers_sasl argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a \"kafka_brokers_sasl\" parameter as an array of Message Hub brokers.")
    )

    runActionWithExpectedResult(actionName, "dat/missingBrokers.json", expectedOutput, false)
  }

  it should "reject invocation when kafka_admin_url argument is missing" in {
    val expectedOutput = JsObject("error" -> JsString(
      "You must supply a \"kafka_admin_url\" parameter.")
    )

    runActionWithExpectedResult(actionName, "dat/missingAdminURL.json", expectedOutput, false)
  }

  it should "reject invocation when user argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a \"user\" parameter to authenticate with Message Hub.")
    )

    runActionWithExpectedResult(actionName, "dat/missingUser.json", expectedOutput, false)
  }

  it should "reject invocation when password argument is missing" in  {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a \"password\" parameter to authenticate with Message Hub.")
    )

    runActionWithExpectedResult(actionName, "dat/missingPassword.json", expectedOutput, false)
  }

  it should "reject invocation when package_endpoint argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("Could not find the package_endpoint parameter.")
    )

    runActionWithExpectedResult(actionName, "dat/missingPackageEndpoint.json", expectedOutput, false)
  }

  it should "reject invocation when isJSONData and isBinaryValue are both enable" in {
    val expectedOutput = JsObject(
      "error" -> JsString("isJSONData and isBinaryValue cannot both be enabled.")
    )

    runActionWithExpectedResult(actionName, "dat/multipleValueTypes.json", expectedOutput, false)
  }

  it should "fire a trigger when a binary message is posted to message hub" in withAssetCleaner(wskprops) {
      val currentTime = s"${System.currentTimeMillis}"

      (wp, assetHelper) =>
          val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
          println(s"Creating trigger ${triggerName}")

          val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
              (trigger, _) =>
                  trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
                      "user" -> kafkaUtils.getAsJson("user"),
                      "password" -> kafkaUtils.getAsJson("password"),
                      "api_key" -> kafkaUtils.getAsJson("api_key"),
                      "kafka_admin_url" -> kafkaUtils.getAsJson("kafka_admin_url"),
                      "kafka_brokers_sasl" -> kafkaUtils.getAsJson("brokers"),
                      "topic" -> topic.toJson,
                      "isBinaryKey" -> true.toJson,
                      "isBinaryValue" -> true.toJson))
          }

          withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
              activation =>
                  // should be successful
                  activation.response.success shouldBe true
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
          val activations = wsk.activation.pollFor(N = 1, Some(triggerName), retries = 60)
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
          val messages = KafkaUtils.messagesInActivation(activation, field="value", value=encodedCurrentTime)
          assert(messages.length == 1)

          val message = messages.head
          message.getFieldPath("topic") shouldBe Some(topic.toJson)
          message.getFieldPath("key") shouldBe Some(encodedKey.toJson)
  }
}
