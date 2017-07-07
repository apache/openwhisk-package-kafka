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

import org.junit.runner.RunWith

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import com.jayway.restassured.RestAssured
import com.jayway.restassured.config.SSLConfig

import common.Wsk
import common.WskProps
import common.TestUtils.FORBIDDEN

import spray.json._

@RunWith(classOf[JUnitRunner])
class KafkaFeedWebTests
  extends FlatSpec
    with BeforeAndAfter
    with Matchers {

  val wskprops = WskProps()

  val webAction = "/whisk.system/messagingWeb/kafkaFeedWeb"
  val webActionURL = s"https://${wskprops.apihost}/api/v1/web${webAction}.http"

  val completeParams = JsObject(
    "triggerName" -> JsString("/invalidNamespace/invalidTrigger"),
    "topic" -> JsString("someTopic"),
    "brokers" -> JsArray(JsString("someBroker")),
    "user" -> JsString("someUsername"),
    "password" -> JsString("somePassword"),
    "kafka_admin_url" -> JsString("https://kafka-admin-prod01.messagehub.services.us-south.bluemix.net:443"),
    "authKey" -> JsString("DoesNotWork")
  )

  def makePutCallWithExpectedResult(params: JsObject, expectedResult: String, expectedCode: Int) = {
    val response = RestAssured.given()
        .contentType("application/json\r\n")
        .config(RestAssured.config().sslConfig(new SSLConfig().relaxedHTTPSValidation()))
        .body(params.toString())
        .put(webActionURL)
    assert(response.statusCode() == expectedCode)
    response.body.asString shouldBe expectedResult
  }

  def makeDeleteCallWithExpectedResult(expectedResult: String, expectedCode: Int) = {
    val response = RestAssured.given().contentType("application/json\r\n").config(RestAssured.config().sslConfig(new SSLConfig().relaxedHTTPSValidation())).delete(webActionURL)
    assert(response.statusCode() == expectedCode)
    response.body.asString shouldBe expectedResult
  }

  behavior of "Kafka feed web action"

  it should "not be obtainable using the CLI" in {
      val wsk = new Wsk()
      implicit val wp = wskprops

      wsk.action.get(webAction, FORBIDDEN)
  }

  it should "reject post of a trigger due to missing brokers argument" in {
    val params = JsObject(completeParams.fields - "brokers")

    makePutCallWithExpectedResult(params, "You must supply a 'brokers' parameter.", 400)
  }

  it should "reject post of a trigger due to missing topic argument" in {
    val params = JsObject(completeParams.fields - "topic")

    makePutCallWithExpectedResult(params, "You must supply a 'topic' parameter.", 400)
  }

  it should "reject post of a trigger due to missing triggerName argument" in {
    val params = JsObject(completeParams.fields - "triggerName")

    makePutCallWithExpectedResult(params, "You must supply a 'triggerName' parameter.", 400)
  }

  it should "reject put of a trigger when authentication fails" in {
    makePutCallWithExpectedResult(completeParams, "You are not authorized for this trigger.", 401)
  }

  // it should "reject delete of a trigger that does not exist" in {
  //   val expectedJSON = JsObject(
  //     "triggerName" -> JsString("/invalidNamespace/invalidTrigger"),
  //     "error" -> JsString("not found")
  //   )
  //
  //   makeDeleteCallWithExpectedResult(expectedJSON, 404)
  // }
}
