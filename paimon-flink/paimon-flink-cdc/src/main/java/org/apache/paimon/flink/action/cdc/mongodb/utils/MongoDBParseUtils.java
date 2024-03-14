/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action.cdc.mongodb.utils;

import org.bson.BsonBinarySubType;
import org.bson.UuidRepresentation;
import org.bson.internal.UuidHelper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Utility to format Bson Object. */
public class MongoDBParseUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBParseUtils.class);

    private static Object parseBsonObject(JSONObject jsonObject) {
        Object result;
        if (jsonObject.has("$date")) {
            Instant instant = Instant.ofEpochMilli(jsonObject.getLong("$date"));
            ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            result = formatter.format(zdt);
        } else if (jsonObject.has("$oid")) {
            result = jsonObject.getString("$oid");
        } else if (jsonObject.has("$numberLong")) {
            result = jsonObject.getString("$numberLong");
        } else if (jsonObject.has("$numberDecimal")) {
            result = jsonObject.getString("$numberDecimal");
        } else if (jsonObject.has("$binary")) {
            byte[] binaryValue =
                    Base64.getDecoder()
                            .decode(
                                    jsonObject
                                            .getString("$binary")
                                            .getBytes(StandardCharsets.UTF_8));
            byte binaryType = (byte) (Integer.parseInt(jsonObject.getString("$type")) & 0xff);
            UuidRepresentation uuidRepresentation = getUuidRepresentation(binaryType);
            result =
                    UuidHelper.decodeBinaryToUuid(binaryValue, binaryType, uuidRepresentation)
                            .toString();
        } else {
            for (Iterator<String> it = jsonObject.keys(); it.hasNext(); ) {
                String key = it.next();
                if (key.contains("$")) {
                    LOG.warn(String.format("data type: %s is not support yet", key));
                }
            }
            result = parseDocument(jsonObject.toString());
        }
        return result;
    }

    private static UuidRepresentation getUuidRepresentation(byte binaryType) {
        BsonBinarySubType subType = BsonBinarySubType.values()[binaryType];
        switch (subType) {
            case BINARY:
                return UuidRepresentation.UNSPECIFIED;
            case UUID_LEGACY:
                return UuidRepresentation.PYTHON_LEGACY;
            case UUID_STANDARD:
                return UuidRepresentation.JAVA_LEGACY;
            default:
                return UuidRepresentation.STANDARD;
        }
    }

    public static Map<String, String> parseDocument(String document) {
        JSONObject jsonObject = new JSONObject(document);
        Map<String, String> resultJsonObject = new HashMap<>();
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = jsonObject.get(key);
            if (jsonObject.get(key) instanceof JSONObject) {
                JSONObject nestedJsonObject = (JSONObject) jsonObject.get(key);
                Object result = parseBsonObject(nestedJsonObject);
                resultJsonObject.put(key, result.toString());
            } else if (jsonObject.get(key) instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) jsonObject.get(key);
                List<Object> lstObject = new ArrayList<>();
                for (int i = 0; i < jsonArray.length(); i++) {
                    Object nestedObject = jsonArray.get(i);
                    if (nestedObject instanceof JSONObject) {
                        JSONObject nestedJsonObject = (JSONObject) nestedObject;
                        lstObject.add(parseBsonObject(nestedJsonObject));
                    } else {
                        lstObject.add(nestedObject);
                    }
                }
                resultJsonObject.put(key, lstObject.toString());
            } else {
                resultJsonObject.put(key, value.toString());
            }
        }
        return resultJsonObject;
    }
}
