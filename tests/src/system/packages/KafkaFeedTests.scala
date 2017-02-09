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

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import spray.json._

import common.JsHelpers
import common.TestHelpers
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import ActionHelper._

@RunWith(classOf[JUnitRunner])
class KafkaFeedTests
  extends FlatSpec
    with Matchers
    with WskActorSystem
    with BeforeAndAfterAll
    with TestHelpers
    with WskTestHelpers
    with JsHelpers {

  implicit val wskprops = WskProps()
  val wsk = new Wsk()
  val actionName = "kafkaFeedAction"
  val actionFile = "../action/kafkaFeed.js"

  behavior of "Kafka feed action"

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

  it should "reject invocation when brokers argument is missing" in  {
    val expectedOutput = JsObject(
      "error" -> JsString("You must supply a \"brokers\" parameter as an array of Kafka brokers.")
    )

    runActionWithExpectedResult(actionName, "dat/missingBrokers.json", expectedOutput, false)
  }

  it should "reject invocation when package_endpoint argument is missing" in {
    val expectedOutput = JsObject(
      "error" -> JsString("Could not find the package_endpoint parameter.")
    )

    runActionWithExpectedResult(actionName, "dat/missingPackageEndpoint.json", expectedOutput, false)
  }

  it should "reject invocation when isJSONData and isBinaryData are both enable" in {
    val expectedOutput = JsObject(
      "error" -> JsString("isJSONData and isBinaryData cannot both be enabled.")
    )

    runActionWithExpectedResult(actionName, "dat/multipleValueTypes.json", expectedOutput, false)
  }
}
