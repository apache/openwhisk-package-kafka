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
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import common.JsHelpers
import common.TestHelpers
import common.StreamLogging
import common.WhiskProperties
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.database.test.ExtendedCouchDbRestClient
import org.apache.openwhisk.utils.{JsHelpers, retry}

import scala.concurrent.Await

@RunWith(classOf[JUnitRunner])
class MessageHubMultiWorkersTest extends FlatSpec
  with Matchers
  with WskActorSystem
  with BeforeAndAfterAll
  with TestHelpers
  with WskTestHelpers
  with JsHelpers
  with StreamLogging
  with KafkaUtils {

  val topic = "test"

  implicit val wskprops = WskProps()
  val wsk = new Wsk()

  val messagingPackage = "/whisk.system/messaging"
  val messageHubFeed = "messageHubFeed"
  val dbProtocol = WhiskProperties.getProperty("db.protocol")
  val dbHost = WhiskProperties.getProperty("db.host")
  val dbPort = WhiskProperties.getProperty("db.port").toInt
  val dbUsername = WhiskProperties.getProperty("db.username")
  val dbPassword = WhiskProperties.getProperty("db.password")
  val dbPrefix = WhiskProperties.getProperty(WhiskConfig.dbPrefix)
  val dbName = s"${dbPrefix}ow_kafka_triggers"
  val client = new ExtendedCouchDbRestClient(dbProtocol, dbHost, dbPort, dbUsername, dbPassword, dbName)

  behavior of "Message Hub Feed"

  ignore should "assign two triggers to same worker when only worker0 is available" in withAssetCleaner(wskprops) {

    (wp, assetHelper) =>
      val firstTrigger = s"firstTrigger-${System.currentTimeMillis()}"
      val secondTrigger = s"secondTrigger-${System.currentTimeMillis()}"

      val worker0 = s"worker${System.currentTimeMillis()}"

      val parameters = constructParams(List(worker0))

      createTrigger(assetHelper, firstTrigger, parameters)
      createTrigger(assetHelper, secondTrigger, parameters)

      retry({
        val result = Await.result(client.getAllDocs(includeDocs = Some(true)), 15.seconds)
        result should be('right)
        val documents = result.right.get.fields("rows").convertTo[List[JsObject]]

        validateTriggerAssignment(documents, firstTrigger, worker0)
        validateTriggerAssignment(documents, secondTrigger, worker0)
      })
  }

  ignore should "assign a trigger to worker0 and a trigger to worker1 when both workers are available" in withAssetCleaner(wskprops) {

    (wp, assetHelper) =>
      val firstTrigger = s"firstTrigger-${System.currentTimeMillis()}"
      val secondTrigger = s"secondTrigger-${System.currentTimeMillis()}"

      val worker0 = s"worker${System.currentTimeMillis()}"
      val worker1 = s"worker${System.currentTimeMillis()}"

      val parameters = constructParams(List(worker0, worker1))

      createTrigger(assetHelper, firstTrigger, parameters)
      createTrigger(assetHelper, secondTrigger, parameters)

      retry({
        val result = Await.result(client.getAllDocs(includeDocs = Some(true)), 15.seconds)
        result should be('right)
        val documents = result.right.get.fields("rows").convertTo[List[JsObject]]

        validateTriggerAssignment(documents, firstTrigger, worker0)
        validateTriggerAssignment(documents, secondTrigger, worker1)
      })
  }

  ignore should "assign a trigger to worker1 when worker0 is removed and there is an assignment imbalance" in withAssetCleaner(wskprops) {

    (wp, assetHelper) =>
      val firstTrigger = s"firstTrigger-${System.currentTimeMillis()}"
      val secondTrigger = s"secondTrigger-${System.currentTimeMillis()}"
      val thirdTrigger = s"thirdTrigger-${System.currentTimeMillis()}"
      val fourthTrigger = s"fourthTrigger-${System.currentTimeMillis()}"

      val worker0 = s"worker${System.currentTimeMillis()}"
      val worker1 = s"worker${System.currentTimeMillis()}"

      val parameters = constructParams(List(worker1))

      createTrigger(assetHelper, firstTrigger, parameters)
      createTrigger(assetHelper, secondTrigger, parameters)
      createTrigger(assetHelper, thirdTrigger, parameters = constructParams(List(worker0, worker1)))
      createTrigger(assetHelper, fourthTrigger, parameters = constructParams(List(worker1)))

      retry({
        val result = Await.result(client.getAllDocs(includeDocs = Some(true)), 15.seconds)
        result should be('right)
        val documents = result.right.get.fields("rows").convertTo[List[JsObject]]

        validateTriggerAssignment(documents, firstTrigger, worker1)
        validateTriggerAssignment(documents, secondTrigger, worker1)
        validateTriggerAssignment(documents, thirdTrigger, worker0)
        validateTriggerAssignment(documents, fourthTrigger, worker1)
      })
  }

  ignore should "balance the load across workers when a worker is added" in withAssetCleaner(wskprops) {

    (wp, assetHelper) =>
      val firstTrigger = s"firstTrigger-${System.currentTimeMillis()}"
      val secondTrigger = s"secondTrigger-${System.currentTimeMillis()}"
      val thirdTrigger = s"thirdTrigger-${System.currentTimeMillis()}"
      val fourthTrigger = s"fourthTrigger-${System.currentTimeMillis()}"
      val fifthTrigger = s"fifthTrigger-${System.currentTimeMillis()}"
      val sixthTrigger = s"sixthTrigger-${System.currentTimeMillis()}"

      val worker0 = s"worker${System.currentTimeMillis()}"
      val worker1 = s"worker${System.currentTimeMillis()}"

      val parameters = constructParams(List(worker0))
      val updatedParameters = constructParams(List(worker0, worker1))

      createTrigger(assetHelper, firstTrigger, parameters)
      createTrigger(assetHelper, secondTrigger, parameters)
      createTrigger(assetHelper, thirdTrigger, updatedParameters)
      createTrigger(assetHelper, fourthTrigger, updatedParameters)
      createTrigger(assetHelper, fifthTrigger, updatedParameters)
      createTrigger(assetHelper, sixthTrigger, updatedParameters)

      retry({
        val result = Await.result(client.getAllDocs(includeDocs = Some(true)), 15.seconds)
        result should be('right)
        val documents = result.right.get.fields("rows").convertTo[List[JsObject]]

        validateTriggerAssignment(documents, firstTrigger, worker0)
        validateTriggerAssignment(documents, secondTrigger, worker0)
        validateTriggerAssignment(documents, thirdTrigger, worker1)
        validateTriggerAssignment(documents, fourthTrigger, worker1)
        validateTriggerAssignment(documents, fifthTrigger, worker0)
        validateTriggerAssignment(documents, sixthTrigger, worker1)
      })
  }

  def constructParams(workers: List[String]) = {
    Map(
      "user" -> getAsJson("user"),
      "password" -> getAsJson("password"),
      "api_key" -> getAsJson("api_key"),
      "kafka_admin_url" -> getAsJson("kafka_admin_url"),
      "kafka_brokers_sasl" -> getAsJson("brokers"),
      "topic" -> topic.toJson,
      "workers" -> workers.toJson
    )
  }

  def validateTriggerAssignment(documents: List[JsObject], trigger: String, worker: String) = {
    val doc = documents.filter(_.fields("id").convertTo[String].contains(trigger))
    JsHelpers.getFieldPath(doc(0), "doc", "worker") shouldBe Some(JsString(worker))
  }
}
