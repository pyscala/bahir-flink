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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An at-least-once Table sink for ClickHouse.
 *
 * <p>The mechanisms of Flink guarantees delivering messages at-least-once to this sink (if
 * checkpointing is enabled). However, one common use case is to run idempotent queries
 * (e.g., <code>REPLACE</code> or <code>INSERT OVERWRITE</code>) to upsert into the database and
 * achieve exactly-once semantic.</p>
 */
public class ClickHouseTableSink implements AppendStreamTableSink<Row> {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseTableSink.class);
    private static final Integer BATCH_SIZE_DEFAULT = 5000;
    private static final Long COMMIT_PADDING_DEFAULT = 5000L;
    private static final Integer RETRIES_DEFAULT = 3;
    private static final Long RETRY_INTERVAL_DEFAULT = 3000L;
    private static final Boolean IGNORE_INSERT_ERROR = false;
    private String address;
    private String username;
    private String password;
    private String database;
    private String table;
    private TableSchema schema;
    private Integer batchSize;
    private Long commitPadding;
    private Integer retries;
    private Long retryInterval;
    private Boolean ignoreInsertError;


    public ClickHouseTableSink(String address, String username, String password, String database, String table, TableSchema schema, Integer batchSize, Long commitPadding, Integer retries, Long retryInterval, Boolean ignoreInsertError) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
        this.schema = schema;
        this.batchSize = batchSize;
        this.commitPadding = commitPadding;
        this.retries = retries;
        this.retryInterval = retryInterval;
        this.ignoreInsertError = ignoreInsertError;

    }

    /**
     * @param dataStream
     * @deprecated
     */
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        consumeDataStream(dataStream);
    }

    /**
     *
     * @return ClickHouseAppendSinkFunction
     */
    private ClickHouseAppendSinkFunction initSink() {
        String prepareStatement = createPrepareStatement(schema, database, table);
        return new ClickHouseAppendSinkFunction(address, username, password, prepareStatement, batchSize, commitPadding, retries, retryInterval, ignoreInsertError);
    }

    @Override
    public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {

        ClickHouseTableSink copy;
        try {
            copy = new ClickHouseTableSink(address, username, password, database, table, schema, batchSize, commitPadding, retries, retryInterval, ignoreInsertError);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return copy;
    }

    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(initSink())
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames()));
    }

    @Override
    public DataType getConsumedDataType() {
        return schema.toRowDataType();
    }


    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
    }


    @Override
    public TableSchema getTableSchema() {
        return schema;
    }


    public static Builder builder() {
        return new Builder();
    }


    public String createPrepareStatement(TableSchema tableSchema, String database, String table) {
        String[] fieldNames = tableSchema.getFieldNames();
        String columns = String.join(",", fieldNames);
        String[] questionMark = new String[fieldNames.length];
        for (int i = 0; i < questionMark.length; i++) {
            questionMark[i] = "?";
        }
        String values = String.join(",", questionMark);
        StringBuilder builder = new StringBuilder("insert into ");
        builder.append(database).append(".")
                .append(table).append(" ( ")
                .append(columns).append(" ) values ( ").append(values).append(" ) ");
        return builder.toString();

    }

    public static class Builder {

        private String address;
        private String username;
        private String password;
        private String database;
        private String table;
        private TableSchema schema;
        private Integer batchSize = BATCH_SIZE_DEFAULT;
        private Long commitPadding = COMMIT_PADDING_DEFAULT;

        private Integer retries = RETRIES_DEFAULT;
        private Long retryInterval = RETRY_INTERVAL_DEFAULT;
        private Boolean ignoreInsertError = IGNORE_INSERT_ERROR;

        public Builder setAddress(String address) {
            this.address = address;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setCommitPadding(Long commitPadding) {
            this.commitPadding = commitPadding;
            return this;
        }

        public Builder setRetries(Integer retries) {
            this.retries = retries;
            return this;

        }

        public Builder setRetryInterval(Long retryInterval) {
            this.retryInterval = retryInterval;
            return this;

        }

        public Builder setIgnoreInsertError(Boolean ignoreInsertError) {
            this.ignoreInsertError = ignoreInsertError;
            return this;
        }

        public ClickHouseTableSink builder() {
            checkNotNull(address, "No address supplied.");
            checkNotNull(username, "No username supplied.");
            checkNotNull(password, "No password supplied.");
            checkNotNull(database, "No database supplied.");
            checkNotNull(table, "No table supplied.");
            checkNotNull(schema, "No table-schema supplied.");
            return new ClickHouseTableSink(address, username, password, database, table, schema, batchSize, commitPadding, retries, retryInterval, ignoreInsertError);
        }


    }
}
