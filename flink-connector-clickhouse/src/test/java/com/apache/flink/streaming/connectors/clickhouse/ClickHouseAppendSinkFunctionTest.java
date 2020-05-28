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

import org.apache.flink.types.Row;
import org.junit.Test;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by liufangliang on 2020/4/16.
 */
public class ClickHouseAppendSinkFunctionTest {


	private static final String USERNAME = "user";
	private static final String PASSWORD = "password";
	private Connection connection;
	private BalancedClickhouseDataSource dataSource;
	private  PreparedStatement pstat;

	@Test
	public void open() throws Exception {
	}

	@Test
	public void invoke() throws Exception {
		Properties properties = new Properties();
		properties.setProperty(USERNAME, "admin");
		properties.setProperty(PASSWORD, "admin");
		ClickHouseProperties clickHouseProperties = new ClickHouseProperties(properties);
		dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://localhost:8123/default", clickHouseProperties);
		connection = dataSource.getConnection();
		pstat = connection.prepareStatement("");
		Row value = new Row(2);
		for (int i = 0; i < value.getArity(); i++) {
			pstat.setObject(i + 1, value.getField(i));
		}


	}

	@Test
	public void doExecuteRetries() throws Exception {
	}

	@Test
	public void snapshotState() throws Exception {
	}

	@Test
	public void initializeState() throws Exception {
	}

}
