/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Map;

import static com.apache.flink.table.descriptors.ClickHouseValidator.*;


/**
 * Created by liufangliang on 2020/4/16.
 */
public class ClickHouseTableSourceSinkFactoryTest {


    @Test
    public void createStreamTableSink() throws Exception {

        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_CLICKHOUSE);
        properties.putString(CONNECTOR_VERSION, "1");
        properties.putString(CONNECTOR_ADRESS, "jdbc:clickhouse://localhost:8123/default");
        properties.putString(CONNECTOR_DATABASE, "qtt");
        properties.putString(CONNECTOR_TABLE, "insert_test");
        properties.putString(CONNECTOR_USERNAME, "admin");
        properties.putString(CONNECTOR_PASSWORD, "admin");
        properties.putString(CONNECTOR_COMMIT_BATCH_SIZE, "1");
        properties.putString(CONNECTOR_COMMIT_PADDING, "3000");
        properties.putString(CONNECTOR_COMMIT_RETRY_ATTEMPTS, "3");
        properties.putString(CONNECTOR_COMMIT_RETRY_INTERVAL, "3000");
        properties.putString(CONNECTOR_COMMIT_IGNORE_ERROR, "false");

        Schema schema = new Schema().field("s", DataTypes.STRING()).field("d", DataTypes.BIGINT());
        Map<String, String> stringStringMap = schema.toProperties();


        properties.putProperties(stringStringMap);


        Row row = new Row(2);
        row.setField(0, "ss");
        row.setField(1, 88);
        ClickHouseTableSourceSinkFactory factory = new ClickHouseTableSourceSinkFactory();
        StreamTableSink<Row> streamTableSink = factory.createStreamTableSink(properties.asMap());
        DataStream<Row> dataStream = StreamExecutionEnvironment.getExecutionEnvironment().fromElements(row);
        streamTableSink.consumeDataStream(dataStream);
    }


}
