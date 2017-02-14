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

import scala.collection.mutable.ListBuffer

import spray.json.DefaultJsonProtocol._
import spray.json.pimpAny


class KafkaUtils {
    lazy val messageHubProps = KafkaUtils.initializeMessageHub()

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

        Map(user, password, kafka_admin_url, api_key, brokers, security_protocol, keySerializer, valueSerializer)
    }
}
