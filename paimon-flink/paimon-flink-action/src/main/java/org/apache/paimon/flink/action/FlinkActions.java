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

package org.apache.paimon.flink.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.ActionFactory.printDefaultHelp;

/** Table maintenance actions for Flink. */
public class FlinkActions {

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printDefaultHelp();
            System.exit(1);
        }
        args = replaceArgsWithEnvVariables(args);
        args = replaceDbConfig(args);
        Optional<Action> action = ActionFactory.createAction(args);

        if (action.isPresent()) {
            action.get().run();
        } else {
            System.exit(1);
        }
    }

    private static String[] replaceArgsWithEnvVariables(String[] args) {
        return Arrays.stream(args)
                .map(
                        arg -> {
                            if (arg.contains("$")) {
                                // Check for values containing environment variables (like
                                // "hosts=$MONGODB_HOST")
                                int dollarIndex = arg.indexOf('$');
                                String prefix = arg.substring(0, dollarIndex);
                                String envVar = arg.substring(dollarIndex + 1);
                                String envValue = System.getenv(envVar);
                                return envValue != null ? prefix + envValue : arg;
                            } else {
                                return arg;
                            }
                        })
                .toArray(String[]::new);
    }

    private static String[] replaceDbConfig(String[] args) {
        ArrayList<String> cmd = new ArrayList<String>();
        for (String arg : args) {
            if (arg.startsWith("mongo_uri")) {
                String mongoUri = arg.split("=")[1];
                Map<String, String> parseUri = parseMongoUri(mongoUri);
                cmd.add(String.format("hosts=%s", parseUri.get("hosts")));
                cmd.add("--mongodb_conf");
                cmd.add(String.format("username=%s", parseUri.get("username")));
                cmd.add("--mongodb_conf");
                cmd.add(String.format("password=%s", parseUri.get("password")));
            } else if (arg.startsWith("mysql_uri")) {
                String mongoUri = arg.split("=")[1];
                Map<String, String> parseUri = parseMySqlUri(mongoUri);
                cmd.add(String.format("hostname=%s", parseUri.get("hosts")));
                cmd.add("--mysql_conf");
                cmd.add(String.format("username=%s", parseUri.get("username")));
                cmd.add("--mysql_conf");
                cmd.add(String.format("password=%s", parseUri.get("password")));
            } else {
                cmd.add(arg);
            }
        }
        return cmd.toArray(new String[0]);
    }

    private static Map<String, String> parseMongoUri(String mongoUri) {
        String regex = "^mongodb://([^:]+):([^@]+)@([^/]+)/.*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(mongoUri);
        Map<String, String> parseUri = new HashMap<>();
        if (matcher.matches()) {
            parseUri.put("username", matcher.group(1));
            parseUri.put("password", matcher.group(2).replace("%40", "@"));
            parseUri.put("hosts", matcher.group(3));
            return parseUri;
        } else {
            throw new IllegalArgumentException("Invalid MongoDB URI format: " + mongoUri);
        }
    }

    private static Map<String, String> parseMySqlUri(String mysqlUri) {
        String regex = "^mysql\\+pool\\+retry://([^:]+):([^@]+)@([^:]+):([^/]+)/.*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(mysqlUri);
        Map<String, String> parseUri = new HashMap<>();

        if (matcher.matches()) {
            parseUri.put("username", matcher.group(1));
            parseUri.put("password", matcher.group(2).replace("%40", "@"));
            parseUri.put("hosts", matcher.group(3));

            return parseUri;
        } else {
            throw new IllegalArgumentException("Invalid MySQL URI format: " + mysqlUri);
        }
    }
}
