/*
 * Copyright 2016 IBM Corporation
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
package system.provider

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import com.jayway.restassured.RestAssured
import common.WhiskProperties
import spray.json._

@RunWith(classOf[JUnitRunner])
class MessagingServiceTests
  extends FlatSpec
    with BeforeAndAfter
    with Matchers {

  val healthEndpoint = s"/health"

  val getMessagingAddress =
    if (System.getProperty("host") != "" && System.getProperty("port") != "") {
      "http://" + System.getProperty("host") + ":" + System.getProperty("port")
    }

  def makePutCallWithExpectedResult(url: String, params: JsObject, expectedResult: JsObject, expectedCode: Int) = {
    val response = RestAssured.given().body(params.toString()).put(url)
    assert(response.statusCode() == expectedCode)
    response.body.asString.parseJson.asJsObject shouldBe expectedResult
  }

  behavior of "Messaging feed provider endpoint"

  it should "return status code HTTP 200 OK from /health endpoint" in {
    val response = RestAssured.given().get(getMessagingAddress + healthEndpoint)

    assert(response.statusCode() == 200 && response.asString().contains("consumers"))
  }

  it should "reject post of a trigger when missing all arguments" in {
    val expectedJSON = JsObject(
      "error" -> JsString("missing fields: brokers, topic, triggerURL, isMessageHub"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", JsObject(), expectedJSON, 400)
  }

  it should "reject post of a trigger due to missing brokers argument" in {
    val params = JsObject(
      "topic" -> JsString("someTopic"),
      "triggerURL" -> JsString("someURL"),
      "isMessageHub" -> JsBoolean(false)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("missing fields: brokers"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 400)
  }

  it should "reject post of a trigger due to missing topic argument" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "triggerURL" -> JsString("someURL"),
      "isMessageHub" -> JsBoolean(false)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("missing fields: topic"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 400)
  }

  it should "reject post of a trigger due to missing triggerURL argument" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "topic" -> JsString("someTopic"),
      "isMessageHub" -> JsBoolean(false)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("missing fields: triggerURL"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 400)
  }

  it should "reject post of a trigger due to missing isMessageHub argument" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "triggerURL" -> JsString("someURL"),
      "topic" -> JsString("someTopic")
    )
    val expectedJSON = JsObject(
      "error" -> JsString("missing fields: isMessageHub"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 400)
  }

  it should "reject post of a trigger due to missing username argument" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "password" -> JsString("somePassword"),
      "topic" -> JsString("someTopic"),
      "triggerURL" -> JsString("someURL"),
      "isMessageHub" -> JsBoolean(true)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("missing fields: username"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 400)
  }

  it should "reject post of a trigger due to missing password argument" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "username" -> JsString("someUsername"),
      "topic" -> JsString("someTopic"),
      "triggerURL" -> JsString("someURL"),
      "isMessageHub" -> JsBoolean(true)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("missing fields: password"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 400)
  }

  it should "reject post of a trigger due to mismatch between triggerURL and trigger name" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "username" -> JsString("someUsername"),
      "password" -> JsString("somePassword"),
      "topic" -> JsString("someTopic"),
      "triggerURL" -> JsString(s"https://someKey@${WhiskProperties.getEdgeHost}/api/v1/namespaces/invalidNamespace/triggers/someTrigger"),
      "isMessageHub" -> JsBoolean(true)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("trigger and namespace from route must correspond to triggerURL"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress +  "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 409)
  }

  it should "reject post of a trigger due to mismatch between triggerURL and namespace" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "username" -> JsString("someUsername"),
      "password" -> JsString("somePassword"),
      "topic" -> JsString("someTopic"),
      "triggerURL" -> JsString(s"https://someKey@${WhiskProperties.getEdgeHost}/api/v1/namespaces/someNamespace/triggers/invalidTrigger"),
      "isMessageHub" -> JsBoolean(true)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("trigger and namespace from route must correspond to triggerURL"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 409)
  }

  it should "reject post of a trigger when authentication fails" in {
    val params = JsObject(
      "brokers" -> JsArray(JsString("someBroker")),
      "username" -> JsString("someUsername"),
      "password" -> JsString("somePassword"),
      "topic" -> JsString("someTopic"),
      "triggerURL" -> JsString(s"https://someKey@${WhiskProperties.getEdgeHost}/api/v1/namespaces/invalidNamespace/triggers/invalidTrigger"),
      "isMessageHub" -> JsBoolean(true)
    )
    val expectedJSON = JsObject(
      "error" -> JsString("not authorized"),
      "success" -> JsBoolean(false)
    )

    makePutCallWithExpectedResult(getMessagingAddress + "/triggers/invalidNamespace/invalidTrigger", params, expectedJSON, 401)
  }

}
