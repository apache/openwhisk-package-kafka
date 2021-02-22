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
const crypto = require("crypto");

const ALGORITHM_AES_256_GCM = "aes-256-gcm";
const ALGORITHM_NONCE_SIZE_12 = 12;
const ALGORITHM_TAG_SIZE_16 = 16;

module.exports = function (encryptKeyID, encryptKeyValue, encryptFallBackKeyID, encryptFallBackKeyValue, cryptVersion) {

    if (encryptKeyValue) {
        this.encryptKeyValue = decodeKeyValue(encryptKeyValue);
    }
    if (encryptFallBackKeyValue) {
        this.encryptFallBackKeyValue = decodeKeyValue(encryptFallBackKeyValue);
    }

    this.decryptAuth = function (authDBString) {
        if (authDBString) {
            let authDBStringArray = authDBString.split('::');
            if (authDBStringArray.length === 1) {
                return authDBString;
            } else if (authDBStringArray.length > 3) {
                let keyValue;
                let cryptVersionID = authDBStringArray[2];
                let base64NonceAndCiphertext = authDBStringArray[3];
                if (cryptVersionID === encryptKeyID) {
                    keyValue = this.encryptKeyValue;
                } else if (cryptVersionID === encryptFallBackKeyID) {
                    keyValue = this.encryptFallBackKeyValue
                } else {
                    return "";
                }

                // decode and retrieve nonce, ciphertext and tag
                let nonceAndCiphertext = Buffer.from(base64NonceAndCiphertext, "base64");
                let nonce = nonceAndCiphertext.slice(0, ALGORITHM_NONCE_SIZE_12);
                let ciphertext = nonceAndCiphertext.slice(ALGORITHM_NONCE_SIZE_12, nonceAndCiphertext.length - ALGORITHM_TAG_SIZE_16);
                let tag = nonceAndCiphertext.slice(ALGORITHM_NONCE_SIZE_12 + ciphertext.length);
                // create cipher instance and set tag
                let cipher = crypto.createDecipheriv(ALGORITHM_AES_256_GCM, Buffer.from(keyValue, "utf8"), nonce);
                cipher.setAuthTag(tag);
                // decrypt and return
                return Buffer.concat([cipher.update(ciphertext), cipher.final()]).toString("utf8");
            } else {
                return "";
            }
        } else {
            return undefined;
        }
    };

    this.encryptAuth = function (password) {
        if (encryptKeyID) {
            return `::${cryptVersion}::${encryptKeyID}::${encryptPassword(password, this.encryptKeyValue)}`;
        } else if (encryptFallBackKeyID) {
            return `::${cryptVersion}::${encryptFallBackKeyID}::${encryptPassword(password, this.encryptFallBackKeyValue)}`;
        } else {
            return password;
        }
    };

    function encryptPassword(password, keyValue) {
        // generate a 96-bit cipher
        let nonce = crypto.randomBytes(ALGORITHM_NONCE_SIZE_12);
        // create the cipher instance
        let cipher = crypto.createCipheriv(ALGORITHM_AES_256_GCM, Buffer.from(keyValue, "utf8"), nonce);
        // encrypt and prepend nonce
        let ciphertext = Buffer.concat([cipher.update(password), cipher.final()]);
        let nonceAndCiphertext = Buffer.concat([nonce, ciphertext, cipher.getAuthTag()]);
        // return base64 encoded AES encrypted string
        return nonceAndCiphertext.toString("base64");
    }

    function decodeKeyValue(keyValue) {
        // Create a buffer from the string
        let bufferObj = Buffer.from(keyValue, "base64");
        // Encode the Buffer as a utf8 string
        return bufferObj.toString("utf8");
    }

};
