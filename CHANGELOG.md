<!--
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Changelog

## 2.1.0
 + Handle URL params gently (#370)
 + Change nodejs:6 to nodejs:default. (#369)
 + Use Kafka client 1.3.0 (#363)
 + Handle cases when authKey does not exist in DB documents (#366)
 + Retry failed database changes (#365)
 + Do not skip last sequence if an exception occurs (#364)
 + Update existing trigger feeds on create instead of failing (#360)
 + Allow feed to be deleted if trigger does not exist (#359)
 + Change some log levels and allow log level to be set on startup (#357)
 + Do not update last canary everytime a database connection attempt occurs (#356)
 + Disable triggers for invalid auth when using custom auth handler (#354)
 + Ensure proper encoding for improper encoded keys (#353)
 + Use API key for authentication when username is token (#350)
 + Catch Doctor exceptions and do not persist consumer database connections (#343)
 + Disable spare connections (#342)
 + Set running state after brokers are connected (#340)
 + Reset consumer restart counter after 24 hours (#337)

## 2.0.1
 + Prevent parsing floats as Infinity and -Infinity (#332)
 + Upgrade base Python version (#330)

## 2.0.0-incubating

* First Apache Release
