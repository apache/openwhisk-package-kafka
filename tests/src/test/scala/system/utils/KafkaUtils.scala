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

package system.utils

import java.util.HashMap
import java.util.Properties
import java.util.concurrent.{TimeUnit, TimeoutException}

import io.restassured.RestAssured
import io.restassured.config.{RestAssuredConfig, SSLConfig}
import javax.security.auth.login.Configuration
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable.ListBuffer
import spray.json.DefaultJsonProtocol._
import spray.json._
import system.packages.ActionHelper._
import org.apache.openwhisk.utils.JsHelpers

import scala.concurrent.duration.DurationInt
import common.TestHelpers
import common.TestUtils
import common.WskTestHelpers
import common.ActivationResult
import org.apache.openwhisk.utils.retry
import org.apache.kafka.clients.producer.ProducerRecord

trait KafkaUtils extends TestHelpers with WskTestHelpers {
    lazy val messageHubProps = KafkaUtils.initializeMessageHub()

    def createProducer() : KafkaProducer[String, String] = {
        // currently only supporting MH
        new KafkaProducer[String, String](KafkaUtils.asKafkaProducerProps(this.messageHubProps))
    }

    def apply(key : String) = {
        this.messageHubProps.getOrElse(key, "")
    }

    def getAsJson(key : String) = {
        key match {
            case key if key == "brokers" => this(key).asInstanceOf[List[String]].toJson
            case key => this(key).asInstanceOf[String].toJson
        }
    }

    val sslconfig = {
        val inner = new SSLConfig().allowAllHostnames()
        val config = inner.relaxedHTTPSValidation()
        new RestAssuredConfig().sslConfig(config)
    }

    def createTrigger(assetHelper: AssetCleaner, name: String, parameters: Map[String, spray.json.JsValue]): String = {
        println(s"Creating trigger $name")

        val feedCreationResult = assetHelper.withCleaner(wsk.trigger, name) {
            (trigger, _) =>
                trigger.create(name, feed = Some(s"/whisk.system/messaging/messageHubFeed"), parameters = parameters)
        }

        val activation = wsk.parseJsonString(feedCreationResult.stdout.substring(0, feedCreationResult.stdout.indexOf("ok: created trigger"))).convertTo[ActivationResult]

        // should be successful
        activation.response.success shouldBe true

        // It takes a moment for the consumer to fully initialize.
        println("Giving the consumer a moment to get ready")
        Thread.sleep(KafkaUtils.consumerInitTime)

        val uuid = activation.response.result.get.fields.get("uuid").get.toString().replaceAll("\"", "")
        consumerExists(uuid)

        uuid
    }


    def consumerExists(uuid: String) = {
        println("Checking health endpoint(s) for existence of consumer uuid")
        // get /health endpoint(s) and ensure it contains the new uuid
        val healthUrls: Array[String] = System.getProperty("health_url").split("\\s*,\\s*").filterNot(_.isEmpty)
        assert(healthUrls.size != 0)

        retry({
            val uuids: Array[(String, JsValue)] = healthUrls.flatMap(u => {
                val response = RestAssured.given().config(sslconfig).get(u)
                assert(response.statusCode() == 200)

                response.asString()
                  .parseJson
                  .asJsObject
                  .getFields("consumers")
                  .head
                  .convertTo[JsArray]
                  .elements
                  .flatMap(c => {
                      val consumer = c.asJsObject.fields.head
                      consumer match {
                          case (u, v) if u == uuid && v.asJsObject.getFields("currentState").head == "Running".toJson => Some(consumer)
                          case _ => None
                      }
                  })
            })

            assert(uuids.nonEmpty)
        }, N = 60, waitBeforeRetry = Some(1.second))
    }

    def produceMessage(topic: String, key: String, value: String) = {
        println(s"Producing message with key: $key and value: $value")
        val producer = createProducer()
        val record = new ProducerRecord(topic, key, value)
        val future = producer.send(record)

        producer.flush()
        producer.close()

        try {
          val result = future.get(60, TimeUnit.SECONDS)

          println(s"Produced message to topic: ${result.topic()} on partition: ${result.partition()} at offset: ${result.offset()} with timestamp: ${result.timestamp()}.")
        } catch {
          case e: TimeoutException =>
            fail(s"TimeoutException received waiting for message to be produced to topic: $topic with key: $key and value: $value. ${e.getMessage}")
          case e: Exception => throw e
        }
    }
}

object KafkaUtils {
    val consumerInitTime = 10000 // ms

    def asKafkaProducerProps(props : Map[String,Object]) : Properties = {
        val requiredKeys = List("brokers",
                                "user",
                                "password",
                                "key.serializer",
                                "value.serializer",
                                "security.protocol",
                                "max.request.size")

        val propertyMap = props.filterKeys(
            requiredKeys.contains(_)
        ).map(
            tuple =>
                tuple match {
                    // transform "brokers" key to "bootstrap.servers"
                    case (k, v) if k == "brokers" => ("bootstrap.servers", v.asInstanceOf[List[String]].mkString(","))
                    case _ => tuple
                }
        )

        val kafkaProducerProps = new Properties()
        for ((k, v) <- propertyMap) kafkaProducerProps.put(k, v)

        kafkaProducerProps
    }

    def messagesInActivation(activation : JsObject, field: String, value: String) : Array[JsObject] = {
        val messages = JsHelpers.getFieldPath(activation, "response", "result", "messages").getOrElse(JsArray.empty).convertTo[Array[JsObject]]
        messages.filter {
            JsHelpers.getFieldPath(_, field) == Some(value.toJson)
        }
    }

    private def initializeMessageHub() = {
        // get the vcap stuff
        var credentials = TestUtils.getCredentials("message_hub")

        // initialize the set of tuples to go into the resulting Map
        val user = ("user", credentials.get("user").getAsString())
        val password = ("password", credentials.get("password").getAsString())
        val kafka_admin_url = ("kafka_admin_url", credentials.get("kafka_admin_url").getAsString())
        val api_key = ("api_key", credentials.get("api_key").getAsString())
        val security_protocol = ("security.protocol", "SASL_SSL");
        val keySerializer = ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        val valueSerializer = ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        val maxRequestSize = ("max.request.size", "3000000");
        var brokerList = new ListBuffer[String]()
        val jsonArray = credentials.get("kafka_brokers_sasl").getAsJsonArray()
        val brokerIterator = jsonArray.iterator()
        while(brokerIterator.hasNext()) {
            val current = brokerIterator.next().getAsString
            brokerList += current
        }

        val brokers = ("brokers", brokerList.toList)

        System.setProperty("java.security.auth.login.config", "")
        setMessageHubSecurityConfiguration(user._2, password._2)

        Map(user, password, kafka_admin_url, api_key, brokers, security_protocol, keySerializer, valueSerializer, maxRequestSize)
    }

    private def setMessageHubSecurityConfiguration(user: String, password: String) = {
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
}
