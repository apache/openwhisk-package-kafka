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

@RunWith(classOf[JUnitRunner])
class MessagingServiceTests
  extends FlatSpec
    with BeforeAndAfter
    with Matchers {

  val healthEndpoint = "/health"

  val getMessagingAddress =
    if (System.getProperty("host") != "" && System.getProperty("port") != "") {
      "http://" + System.getProperty("host") + ":" + System.getProperty("port")
    }

  behavior of "Messaging feed provider endpoint"

  it should "return status code HTTP 200 OK from /health endpoint" in {
    val response = RestAssured.given().get(getMessagingAddress + healthEndpoint)

    assert(response.statusCode() == 200 && response.asString().contains("consumers"))
  }
}
