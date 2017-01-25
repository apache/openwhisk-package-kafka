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

package system.utils

import common.TestUtils

import java.util.HashMap
import java.util.Properties
import javax.security.auth.login.Configuration
import javax.security.auth.login.AppConfigurationEntry

import org.apache.kafka.clients.producer.KafkaProducer;

import scala.collection.mutable.ListBuffer

import spray.json.DefaultJsonProtocol._
import spray.json.pimpAny


class KafkaUtils {
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
}

object KafkaUtils {
    def asKafkaProducerProps(props : Map[String,Object]) : Properties = {
        val requiredKeys = List("brokers",
                                "user",
                                "password",
                                "key.serializer",
                                "value.serializer",
                                "security.protocol")

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

        Map(user, password, kafka_admin_url, api_key, brokers, security_protocol, keySerializer, valueSerializer)
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
