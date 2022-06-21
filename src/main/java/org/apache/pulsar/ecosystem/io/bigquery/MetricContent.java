/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.bigquery;

/**
 * Bigquery connector all metrics.
 */
public class MetricContent {
    public static final String RECEIVE_COUNT = "receive_count";
    public static final String ACK_COUNT = "ack_count";
    public static final String FAIL_COUNT = "fail_count";
    public static final String RETRY_COUNT = "retry_count";
    public static final String UPDATE_SCHEMA_COUNT = "update_schema_count";
}
