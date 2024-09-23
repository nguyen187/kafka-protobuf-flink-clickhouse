package myflink;

import myflink.message.ExchangeProtoMessage;
import myflink.model.ProtMessageDeserializer;
import myflink.source.AdaptiveWatermarkAssigner;
import myflink.util.ConfigProperty;
import myflink.util.SumIpMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

public class StreamingJobV3 {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(0);

        final ConfigProperty configProperty = ConfigProperty.getInstance();
        final String brokers = configProperty.getConfigString(ConfigProperty.KAFKA_BROKER_LIST);
        final String topic = configProperty.getConfigString(ConfigProperty.GTPV2_S11_TOPIC);
        final String groupId = configProperty.getConfigString(ConfigProperty.GROUP_ID);

        final Long allowedLateness = configProperty.getConfigLong(ConfigProperty.AW_ALLOWED_LATENESS);
        final Double oooThreshold = configProperty.getConfigDouble(ConfigProperty.AW_OOO_THRESHOLD);
        final Double sensitivity = configProperty.getConfigDouble(ConfigProperty.AW_SENSITIVITY);
        final Integer sensitivityCangeRate = configProperty.getConfigInt(ConfigProperty.AW_SENSITiVITY_RATE);
        final Integer windowWidth = configProperty.getConfigInt(ConfigProperty.AW_WINDOW_WIDTH);
        final Integer period = configProperty.getConfigInt(ConfigProperty.AW_PERIOD);

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", groupId);

        FlinkKafkaConsumer<ExchangeProtoMessage.ProtMessage> consumer = new FlinkKafkaConsumer<>(
                topic,
                new ProtMessageDeserializer(),
                properties
        );


        WatermarkStrategy<ExchangeProtoMessage.ProtMessage> watermarkStrategy = WatermarkStrategy
                .<ExchangeProtoMessage.ProtMessage>forGenerator((ctx) -> new AdaptiveWatermarkAssigner(allowedLateness, oooThreshold, sensitivity))
                .withTimestampAssigner((event, timestamp) -> {
                            try {
                                Date date = sdfDate.parse(event.getTimestamp());
                                                    return date.getTime();
                            } catch (ParseException e) {
                                throw new IllegalArgumentException(e);
                            }
                });

        consumer.assignTimestampsAndWatermarks(watermarkStrategy);



//        AdaptiveWatermarkGeneratorSource src = new AdaptiveWatermarkGeneratorSource(topic,properties, allowedLateness, oooThreshold, sensitivity, sensitivityCangeRate);



        DataStream<ExchangeProtoMessage.ProtMessage> stream = env.addSource(consumer);

//        stream.print();

        DataStream<SumIpMessage> sumSizeMessageIp = stream
                .map(new MapFunction<ExchangeProtoMessage.ProtMessage, ExchangeProtoMessage.ProtMessage>() {
                    @Override
                    public ExchangeProtoMessage.ProtMessage map(ExchangeProtoMessage.ProtMessage r) {
                        String[] parts = r.getSourceIp().split("\\.");
                        String newSourceIp = parts[0] + "." + parts[1];
                        return new ExchangeProtoMessage.ProtMessage(newSourceIp,r.getTimestamp(),r.getSize());
                    }
                })
                .keyBy(ExchangeProtoMessage.ProtMessage::getSourceIp)
                .timeWindow(Time.seconds(10))
                .apply(new StreamingJob.SumSizeMessage());

//		System.out.println(sumSizeMessageIp);
        sumSizeMessageIp.print();
//		sumSizeMessageIp.addSink(new ClickHouseSink());
        env.execute("Flink Streaming Java API Skeleton");
    }




    public static class SumSizeMessage implements WindowFunction<ExchangeProtoMessage.ProtMessage, SumIpMessage, String, TimeWindow> {
        @Override
        public 	void apply(String Id, TimeWindow window, Iterable<ExchangeProtoMessage.ProtMessage> vals, Collector<SumIpMessage> out) {
            int cnt = 0;
            double sum = 0.0;
            for (ExchangeProtoMessage.ProtMessage r : vals) {
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
