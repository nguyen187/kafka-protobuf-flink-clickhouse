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

package myflink;

import myflink.model.ProtMessageDeserializer;
import myflink.util.ConfigProperty;

import myflink.util.SumIpMessage;
import myflink.util.TimeUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import myflink.message.ExchangeProtoMessage.ProtMessage;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.forBoundedOutOfOrderness;

public class StreamingJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(100L);

        final ConfigProperty configProperty = ConfigProperty.getInstance();
		final String brokers = configProperty.getConfigString(ConfigProperty.KAFKA_BROKER_LIST);
		final String topic = configProperty.getConfigString(ConfigProperty.GTPV2_S11_TOPIC);
		final String groupId = configProperty.getConfigString(ConfigProperty.GROUP_ID);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", brokers);
		properties.setProperty("group.id", groupId);

		FlinkKafkaConsumer<ProtMessage> consumer = new FlinkKafkaConsumer<>(
				topic,
				new ProtMessageDeserializer(),
				properties
		);

		DataStream<ProtMessage> stream = env.addSource(consumer).assignTimestampsAndWatermarks(WatermarkStrategy
				.<ProtMessage>forBoundedOutOfOrderness(Duration.ofSeconds(5))
				.withTimestampAssigner((event, timestamp) ->
				TimeUtils.parseTimestamp(event.getTimestamp())
		));

		DataStream<SumIpMessage> sumSizeMessageIp = stream
				.map(new MapFunction<ProtMessage,ProtMessage>() {
					@Override
					public ProtMessage map(ProtMessage r) {
						String[] parts = r.getSourceIp().split("\\.");
						String newSourceIp = parts[0] + "." + parts[1];
						return new ProtMessage(newSourceIp,r.getTimestamp(),r.getSize());
					}
				})
				.keyBy(ProtMessage::getSourceIp)
				.timeWindow(Time.seconds(10))
				.apply(new SumSizeMessage());

		System.out.println(sumSizeMessageIp);
		sumSizeMessageIp.addSink(new ClickHouseSink());
		env.execute("Flink Streaming Java API Skeleton");
	}
	public static class SumSizeMessage implements WindowFunction<ProtMessage, SumIpMessage, String, TimeWindow> {
		@Override
		public 	void apply(String Id, TimeWindow window, Iterable<ProtMessage> vals, Collector<SumIpMessage> out) {
			int cnt = 0;
			double sum = 0.0;
			for (ProtMessage r : vals) {
				cnt++;
				sum += r.getSize();
			}
			Instant windowEndInstant = Instant.ofEpochMilli(window.getEnd());
			String windowEndTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
					.withZone(ZoneId.systemDefault())
					.format(windowEndInstant);
			SumIpMessage a = new SumIpMessage(Id, windowEndTime, sum);
            out.collect(new SumIpMessage(Id, windowEndTime, sum));
        }
	}
	public static class ClickHouseSink extends RichSinkFunction<SumIpMessage> {
		private transient Connection connection;
		private transient PreparedStatement statement;

		@Override
		public void open(Configuration parameters) throws Exception {
			String url = "jdbc:clickhouse://localhost:8123/default";

            Properties properties = new Properties();
            properties.setProperty("user", "default");
            properties.setProperty("password", "default");

            connection = DriverManager.getConnection(url, properties);
			String sql = "INSERT INTO sum_ip_message (sourceIp, window, size) VALUES (?, ?, ?)";
			statement = connection.prepareStatement(sql);
		}

		@Override
		public void invoke(SumIpMessage value, Context context) throws Exception {
			// Set parameters and execute update
			statement.setString(1, value.getSourceIp());
			statement.setString(2, value.getWindow());
			statement.setDouble(3, value.getSize());
			statement.executeUpdate();
		}

		@Override
		public void close() throws Exception {
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}

}

