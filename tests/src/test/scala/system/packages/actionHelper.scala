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

import spray.json._

import common.Wsk
import common.WskProps
import common.WskTestHelpers

object ActionHelper extends WskTestHelpers {

  implicit val wskprops = WskProps()
  val wsk = new Wsk()

  def runActionWithExpectedResult(actionName: String, inputFile: String, expectedOutput: JsObject, success: Boolean): Unit = {
    val run = wsk.action.invoke(actionName, parameterFile = Some(inputFile))

    withActivation(wsk.activation, run) {
        activation =>
          activation.response.result shouldBe Some(expectedOutput)
          activation.response.success shouldBe success
      }
    }

}
