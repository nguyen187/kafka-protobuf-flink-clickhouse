package com.viettel.vtnet.traffic;

import com.viettel.vtnet.traffic.clickhouse.ClickHouseSink;
import com.viettel.vtnet.traffic.message.TrafficMessage;
import com.viettel.vtnet.traffic.mapper.IpMapper;
import com.viettel.vtnet.traffic.model.ProtMessageDeserializer;
import com.viettel.vtnet.traffic.util.ConfigProperty;
import com.viettel.vtnet.traffic.util.TimeUtils;
import com.viettel.vtnet.traffic.window.SumSizeMessage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.viettel.vtnet.traffic.message.ExchangeProtoMessage.ProtMessage;

import java.time.Duration;
import java.util.Properties;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(100L);

		final ConfigProperty configProperty = ConfigProperty.getInstance();
		final String topic = configProperty.getConfigString(ConfigProperty.GTPV2_S11_TOPIC);

		Properties kafkaProperties = configProperty.createKafkaInputParameterMap();
		FlinkKafkaConsumer<ProtMessage> consumer = createKafkaConsumer(topic, kafkaProperties);

		DataStream<ProtMessage> stream = env.addSource(consumer)
				.assignTimestampsAndWatermarks(createWatermarkStrategy());

		DataStream<TrafficMessage> sumSizeMessageIp = stream
				.map(new IpMapper())
				.keyBy(ProtMessage::getSourceIp)
				.timeWindow(Time.seconds(10))
				.apply(new SumSizeMessage());

		sumSizeMessageIp.print();
		sumSizeMessageIp.addSink(new ClickHouseSink());

		env.execute("Flink Streaming Java API Skeleton");
	}

	private static FlinkKafkaConsumer<ProtMessage> createKafkaConsumer(String topic, Properties kafkaProperties) {
		return new FlinkKafkaConsumer<>(topic, new ProtMessageDeserializer(), kafkaProperties);
	}

	private static WatermarkStrategy<ProtMessage> createWatermarkStrategy() {
		return WatermarkStrategy
				.<ProtMessage>forBoundedOutOfOrderness(Duration.ofSeconds(5))
				.withTimestampAssigner((event, timestamp) -> TimeUtils.parseTimestamp(event.getTimestamp()))
				.withIdleness(Duration.ofMinutes(1));
	}
}