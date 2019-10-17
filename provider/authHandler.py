"""IAMAuth class.

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
"""

import requests
import time

from requests.auth import AuthBase

class AuthHandlerException(Exception):
    def __init__(self, response):
        self.response = response

class IAMAuth(AuthBase):

    def __init__(self, authKey, endpoint):
        self.authKey = authKey
        self.endpoint = endpoint
        self.tokenInfo = {}

    def __call__(self, r):
        r.headers['Authorization'] = 'Bearer {}'.format(self.__getToken())
        return r

    def __getToken(self):
        if 'expires_in' not in self.tokenInfo or self.__isRefreshTokenExpired():
            response = self.__requestToken()
            if response.ok and 'access_token' in response.json():
                self.tokenInfo = response.json()
                return self.tokenInfo['access_token']
            else:
                raise AuthHandlerException(response)
        elif self.__isTokenExpired():
            response = self.__refreshToken()
            if response.ok and 'access_token' in response.json():
                self.tokenInfo = response.json()
                return self.tokenInfo['access_token']
            else:
                raise AuthHandlerException(response)
        else:
            return self.tokenInfo['access_token']

    def __requestToken(self):
        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic Yng6Yng='
        }
        payload = {
            'grant_type': 'urn:ibm:params:oauth:grant-type:apikey',
            'apikey': self.authKey
        }

        return self.__sendRequest(payload, headers)

    def __refreshToken(self):
        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic Yng6Yng='
        }
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.tokenInfo['refresh_token']
        }

        return self.__sendRequest(payload, headers)


    def __isTokenExpired(self):
        if 'expires_in' not in self.tokenInfo or 'expiration' not in self.tokenInfo:
            return True

        fractionOfTtl = 0.8
        timeToLive = self.tokenInfo['expires_in']
        expireTime = self.tokenInfo['expiration']
        currentTime = int(time.time())
        refreshTime = expireTime - (timeToLive * (1.0 - fractionOfTtl))

        return refreshTime < currentTime

    def __isRefreshTokenExpired(self):
        if 'expiration' not in self.tokenInfo:
            return True

        sevenDays = 7 * 24 * 3600
        currentTime = int(time.time())
        newTokenTime = self.tokenInfo['expiration'] + sevenDays

        return newTokenTime < currentTime

    def __sendRequest(self, payload, headers):
        response = requests.post(self.endpoint, data=payload, headers=headers)
        return response
