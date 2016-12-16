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

import java.util.HashMap
import java.util.Properties
import javax.security.auth.login.Configuration
import javax.security.auth.login.AppConfigurationEntry

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import common.JsHelpers
import common.TestHelpers
import common.TestUtils
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import spray.json.DefaultJsonProtocol.StringJsonFormat
import spray.json.JsArray
import spray.json.JsString
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

    def getNonDefaultNamespace(): String = {
        wsk.namespace.list().stdout.trim.split("\n").last
    }

    def setMessageHubSecurityConfiguration(user: String, password: String) = {
        val map = new HashMap[String, String]()
        map.put("serviceName", "kafka")
        map.put("username", user)
        map.put("password", password)
        Configuration.setConfiguration(new Configuration()
        {
            def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = Array(
    	          new AppConfigurationEntry (
    	              "com.ibm.messagehub.login.MessageHubLoginModule",
     			          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, map))
        })
    }

    behavior of "Message Hub"

    it should "fire a trigger when a message is posted to the message hub" in withAssetCleaner(wskprops) {
        val nondefaultNamespace = getNonDefaultNamespace()
        var credentials = TestUtils.getCredentials("message_hub")
        val user = credentials.get("user").getAsString()
        val password = credentials.get("password").getAsString()
        val kafka_admin_url = credentials.get("kafka_admin_url").getAsString()
        val api_key = credentials.get("api_key").getAsString()
        val kafka_brokers_sasl_json_array = credentials.get("kafka_brokers_sasl").getAsJsonArray()

        var vec = Vector[JsString]()
        var servers = s""
        val iter = kafka_brokers_sasl_json_array.iterator();
        while(iter.hasNext()){
            val server = iter.next().getAsString()
            vec = vec :+ JsString(server)
            servers = s"$servers$server,"
        }
        var kafka_brokers_sasl = JsArray(vec)

        val currentTime = s"${System.currentTimeMillis}"
        System.setProperty("java.security.auth.login.config", "")
        setMessageHubSecurityConfiguration(user, password)
        var props = new Properties()
        props.put("bootstrap.servers", servers);
        props.put("security.protocol", "SASL_SSL");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        (wp, assetHelper) =>
            val triggerName = s"/$nondefaultNamespace/dummyMessageHubTrigger-$currentTime"
            val feedCreationResult = assetHelper.withCleaner(wsk.trigger, triggerName) {
                (trigger, _) =>
                    trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
                        "user" -> user.toJson,
                        "password" -> password.toJson,
                        "api_key" -> api_key.toJson,
                        "kafka_admin_url" -> kafka_admin_url.toJson,
                        "kafka_brokers_sasl" -> kafka_brokers_sasl,
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
            val producer = new KafkaProducer[String, String](props)
            val record = new ProducerRecord(topic, "key", currentTime)
            producer.send(record)
            producer.close()
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
