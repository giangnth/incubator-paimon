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

import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.bson.BsonBinarySubType;
import org.bson.UuidRepresentation;
import org.bson.internal.UuidHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility to format Bson Object. */
public class MongoDBParseUtils {
    protected static final Logger LOG = LoggerFactory.getLogger(MongoDBParseUtils.class);

    private MongoDBParseUtils() {}

    public static Map<String, String> parseDocument(String value) {
        JsonNode jsonNode = JsonSerdeUtil.fromJson(value, new TypeReference<JsonNode>() {});
        String table = jsonNode.get("ns").get("coll").asText();
        String op = jsonNode.get("operationType").asText();
        Map<String, Object> rowData = transformDocument(jsonNode.get("fullDocument"));
        Map<String, String> result = new HashMap<>();
        result.put("table", table);
        result.put("op", op);
        result.put("fullDocument", JsonSerdeUtil.toJson(rowData));
        return result;
    }

    private static Map<String, Object> transformDocument(JsonNode jsonNode)
            throws RuntimeException {
        Map<String, Object> recordMap;
        if (jsonNode.isNull()) {
            return Collections.emptyMap();
        }
        if (jsonNode.isTextual()) {
            try {
                JsonNode textNode =
                        JsonSerdeUtil.asSpecificNodeType(jsonNode.asText(), JsonNode.class);
                recordMap =
                        JsonSerdeUtil.convertValue(
                                textNode, new TypeReference<Map<String, Object>>() {});
            } catch (JsonProcessingException ex) {
                LOG.warn("extractRow error {}", jsonNode);
                return Collections.emptyMap();
            }

        } else {
            recordMap =
                    JsonSerdeUtil.convertValue(
                            jsonNode, new TypeReference<Map<String, Object>>() {});
        }
        if (recordMap == null) {
            return Collections.emptyMap();
        }

        return parserRow(recordMap);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parserRow(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>();
        row.forEach(
                (key, value) -> {
                    if (value instanceof Map) {
                        try {
                            result.put(key, parseBsonObject((Map<String, Object>) value));
                        } catch (RuntimeException e) {
                            throw new RuntimeException(e);
                        }

                    } else if (value instanceof ArrayList) {
                        List<Object> list = (List<Object>) value;
                        List<Object> resultList = new ArrayList<>();
                        for (Object obj : list) {
                            if (obj instanceof Map) {
                                try {
                                    resultList.add(parseBsonObject((Map<String, Object>) obj));
                                } catch (RuntimeException e) {
                                    throw new RuntimeException(e);
                                }

                            } else {
                                resultList.add(obj);
                            }
                        }
                        result.put(key, resultList);
                    } else if (value instanceof Boolean) {
                        result.put(key, value.toString());
                    } else {
                        result.put(key, value);
                    }
                });
        return result;
    }

    private static Object parseBsonObject(Map<String, Object> map) throws RuntimeException {
        if (map.containsKey("$date")) {
            return DateTimeUtils.formatInstant(
                    Instant.ofEpochMilli(Long.parseLong(map.get("$date").toString())));
        } else if (map.containsKey("$oid")) {
            return map.get("$oid").toString();
        } else if (map.containsKey("$numberLong")) {
            return Long.parseLong(map.get("$numberLong").toString());
        } else if (map.containsKey("$numberDecimal")) {
            return getBigDecimal(map.get("$numberDecimal"));
        } else if (map.containsKey("$binary")) {
            byte[] binaryValue =
                    Base64.getDecoder()
                            .decode(map.get("$binary").toString().getBytes(StandardCharsets.UTF_8));
            byte binaryType = (byte) (Integer.parseInt(map.get("$type").toString()) & 0xff);
            UuidRepresentation uuidRepresentation = getUuidRepresentation(binaryType);
            return UuidHelper.decodeBinaryToUuid(binaryValue, binaryType, uuidRepresentation)
                    .toString();
        } else {
            for (String key : map.keySet()) {
                if (key.contains("$")) {
                    throw new RuntimeException(
                            String.format("data type: %s is not supported yet", key));
                }
            }
            return parserRow(map);
        }
    }

    private static BigDecimal getBigDecimal(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof String) {
            return new BigDecimal((String) value);
        } else if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        } else if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            throw new ClassCastException(
                    "Not possible to coerce ["
                            + value
                            + "] from class "
                            + value.getClass()
                            + " into a BigDecimal.");
        }
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
}
