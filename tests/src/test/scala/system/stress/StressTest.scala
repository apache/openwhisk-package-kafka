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

package system.stress

import system.utils.KafkaUtils

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import common.TestHelpers
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import spray.json.DefaultJsonProtocol._
import spray.json._


@RunWith(classOf[JUnitRunner])
class BasicStressTest
    extends FlatSpec
    with Matchers
    with WskActorSystem
    with TestHelpers
    with WskTestHelpers
    with KafkaUtils {

    val topic = "test"
    val sessionTimeout = 10 seconds

    implicit val wskprops = WskProps()
    val wsk = new Wsk()

    val messagingPackage = "/whisk.system/messaging"
    val messageHubFeed = "messageHubFeed"
    val messageHubProduce = "messageHubProduce"

    behavior of "Message Hub provider"

    it should "rapidly create and delete many triggers" in {
        stressTriggerCreateAndDelete(totalIterations = 100, keepNthTrigger = 5)
    }

    /*
     * Recursively create and delete (potentially) lots of triggers
     *
     * @param totalIterations The total number of triggers to create
     * @param keepNthTrigger Optionally, do not delete the trigger created on every N iterations
     * @param currentIteration Used for recursion
     * @param storedTriggers The list of trigger names that were created, but not deleted (see keepNthTrigger)
     */
    def stressTriggerCreateAndDelete(totalIterations : Int, keepNthTrigger : Int, currentIteration : Int = 0, storedTriggers : List[String] = List[String]()) {
        if(currentIteration < totalIterations) {
            val currentTime = s"${System.currentTimeMillis}"

            // use this to print non-zero-based iteration numbers you know... for humans
            val iterationLabel = currentIteration + 1

            val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
            println(s"\nCreating trigger #${iterationLabel}: ${triggerName}")
            val feedCreationResult = wsk.trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
                    "user" -> getAsJson("user"),
                    "password" -> getAsJson("password"),
                    "api_key" -> getAsJson("api_key"),
                    "kafka_admin_url" -> getAsJson("kafka_admin_url"),
                    "kafka_brokers_sasl" -> getAsJson("brokers"),
                    "topic" -> topic.toJson))

            println("Waiting for trigger create")
            withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
                activation =>
                    // should be successful
                    activation.response.success shouldBe true
            }

            // optionally allow triggers to pile up on the provider
            if((iterationLabel % keepNthTrigger) != 0) {
                println("Deleting trigger")
                val feedDeletionResult = wsk.trigger.delete(triggerName)
                feedDeletionResult.stdout should include("ok")
                stressTriggerCreateAndDelete(totalIterations, keepNthTrigger, currentIteration + 1, storedTriggers)
            } else {
                println("I think I'll keep this trigger...")
                stressTriggerCreateAndDelete(totalIterations, keepNthTrigger, currentIteration + 1, triggerName :: storedTriggers)
            }
        } else {
            println("\nCompleted all iterations, now cleaning up stored triggers.")
            for(triggerName <- storedTriggers) {
                println(s"Deleting trigger: ${triggerName}")
                val feedDeletionResult = wsk.trigger.delete(triggerName)
                feedDeletionResult.stdout should include("ok")
            }
        }
    }
}
