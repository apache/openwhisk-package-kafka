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
class MessagingFeedTests
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

    val kafkaUtils = new KafkaUtils

    behavior of "Message Hub"

    it should "fire a trigger when a message is posted to the message hub" in withAssetCleaner(wskprops) {
        val currentTime = s"${System.currentTimeMillis}"

        (wp, assetHelper) =>
            val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
            val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
                (trigger, _) =>
                    trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
                        "user" -> kafkaUtils("user").asInstanceOf[String].toJson,
                        "password" -> kafkaUtils("password").asInstanceOf[String].toJson,
                        "api_key" -> kafkaUtils("api_key").asInstanceOf[String].toJson,
                        "kafka_admin_url" -> kafkaUtils("kafka_admin_url").asInstanceOf[String].toJson,
                        "kafka_brokers_sasl" -> kafkaUtils("brokers").asInstanceOf[List[String]].toJson,
                        "topic" -> topic.toJson))
            }
            withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
                activation =>
                    // should be successful
                    activation.response.success shouldBe true
            }

            // It takes a moment for the consumer to fully initialize. We choose 2 seconds
            // as a temporary length of time to wait for.
            Thread.sleep(2000)

            withActivation(wsk.activation, wsk.action.invoke(s"$messagingPackage/$messageHubProduce", Map(
                "user" -> kafkaUtils("user").asInstanceOf[String].toJson,
                "password" -> kafkaUtils("password").asInstanceOf[String].toJson,
                "kafka_brokers_sasl" -> kafkaUtils("brokers").asInstanceOf[List[String]].toJson,
                "topic" -> topic.toJson,
                "value" -> currentTime.toJson))) {
                    _.response.success shouldBe true
                }

            val activations = wsk.activation.pollFor(N = 2, Some(triggerName), retries = 30)
            var triggerFired = false
            assert(activations.length > 0)

            for (id <- activations) {
                val activation = wsk.activation.waitForActivation(id)
                if (activation.isRight) {
                    // Check if the trigger is fired with the specific message, which is the current time
                    // generated.
                    if (activation.right.get.fields.get("response").toString.contains(currentTime))
                        triggerFired = true
                }
            }
            assert(triggerFired == true)
    }
}
