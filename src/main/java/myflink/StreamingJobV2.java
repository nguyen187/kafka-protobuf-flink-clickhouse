package myflink;

import myflink.model.ProtMessageDeserializer;
import myflink.util.ConfigProperty;
import myflink.util.SumIpMessage;
import myflink.util.TimeUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import myflink.message.ExchangeProtoMessage.ProtMessage;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.forBoundedOutOfOrderness;

public class StreamingJobV2 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Set RocksDB as the state backend
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///tmp/flink/checkpoints", true);
        env.setStateBackend((org.apache.flink.runtime.state.StateBackend) rocksDBStateBackend);

        // Set global parallelism
        env.setParallelism(4);

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

        DataStream<ProtMessage> stream = env.addSource(consumer)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ProtMessage>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) ->
                                TimeUtils.parseTimestamp(event.getTimestamp())
                        ))
                .setParallelism(2); // Set parallelism for the Kafka consumer

        DataStream<SumIpMessage> sumSizeMessageIp = stream
                .map(new MapFunction<ProtMessage, ProtMessage>() {
                    @Override
                    public ProtMessage map(ProtMessage r) {
                        String[] parts = r.getSourceIp().split("\\.");
                        String newSourceIp = parts[0] + "." + parts[1];
                        return new ProtMessage(newSourceIp, r.getTimestamp(), r.getSize());
                    }
                })
                .setParallelism(3) // Set parallelism for the map function
                .keyBy(ProtMessage::getSourceIp)
                .timeWindow(Time.seconds(10))
                .apply(new SumSizeMessage())
                .setParallelism(4); // Set parallelism for the window function

        sumSizeMessageIp.addSink(new ClickHouseSink()).setParallelism(2); // Set parallelism for the ClickHouse sink

        env.execute("Flink Streaming Java API Skeleton");
    }

    // Window function to sum the sizes
    public static class SumSizeMessage implements WindowFunction<ProtMessage, SumIpMessage, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<ProtMessage> input, Collector<SumIpMessage> out) {
            double sum = 0.0;
            for (ProtMessage r : input) {
                sum += r.getSize();
            }
            Instant windowEndInstant = Instant.ofEpochMilli(window.getEnd());
            String windowEndTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(ZoneId.systemDefault())
                    .format(windowEndInstant);
            out.collect(new SumIpMessage(key, windowEndTime, sum));
        }
    }

    // Sink function to write results to ClickHouse
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