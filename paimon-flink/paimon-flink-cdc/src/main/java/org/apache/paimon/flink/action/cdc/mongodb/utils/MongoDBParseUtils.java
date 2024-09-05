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

import com.google.gson.Gson;
import org.bson.BsonBinarySubType;
import org.bson.UuidRepresentation;
import org.bson.internal.UuidHelper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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

    private static final Gson GSON_INSTANCE;

    static {
        GSON_INSTANCE = new Gson();
    }

    private MongoDBParseUtils() {}

    private static Object parseBsonObject(JSONObject jsonObject) {
        if (jsonObject == null) {
            return null;
        }
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
                    throw new JSONException(String.format("data type: %s is not support yet", key));
                }
            }
            result = parseDocument(jsonObject);
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

    private static boolean isNull(Object obj) {
        return obj == null || "null".equals(obj.toString()) || "".equals(obj.toString())
                ? true
                : false;
    }

    public static Map<String, Object> parseDocument(JSONObject jsonObject) {
        Map<String, Object> resultJsonObject = new HashMap<>();
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = jsonObject.get(key);
            if (value instanceof JSONObject) {
                Object nestedJson = parseBsonObject((JSONObject) value);
                resultJsonObject.put(key, nestedJson);

            } else if (value instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) value;
                List<Object> lstObject = new ArrayList<>();
                for (int i = 0; i < jsonArray.length(); i++) {
                    Object nestedObject = jsonArray.get(i);
                    if (nestedObject instanceof JSONObject) {
                        lstObject.add(parseBsonObject((JSONObject) nestedObject));
                    } else {
                        lstObject.add(nestedObject);
                    }
                }
                resultJsonObject.put(key, lstObject);
            } else {
                resultJsonObject.put(key, isNull(value) ? "{}" : value);
            }
        }
        return resultJsonObject;
    }

    public static String parseDocument(String document) {
        return GSON_INSTANCE.toJson(parseDocument(new JSONObject(document)));
    }
}
