package myflink.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import myflink.message.ExchangeProtoMessage.ProtMessage;
import myflink.model.ProtMessageSerializer;

public class KafkaProducerWithProtobufV2 {

    public static void main(String[] args) {

        System.out.println("going to publish messages");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

        Producer<Integer, ProtMessage> producer = new KafkaProducer<>(props, new IntegerSerializer(), new ProtMessageSerializer());

        try {
            for (int i = 0; i < 10; i++) { // Generate 10 random messages
                String line = generateRandomData();
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
                LocalDateTime now = LocalDateTime.now();
                String[] pairs = line.split(",\\s*");

                Map<String, String> dataMap = new HashMap<>();

                for (String pair : pairs) {
                    String[] keyValue = pair.split("=", 2);
                    if (keyValue.length == 2) {
                        dataMap.put(keyValue[0].trim(), keyValue[1].trim());
                    }
                }

                ProtMessage.Builder messageBuilder = ProtMessage.newBuilder()
                        .setTimestamp(dataMap.get("timestamp"))
                        .setSourceIp(dataMap.get("source-ip"))
                        .setDestIp(dataMap.get("dest-ip"))
                        .setMessageType(dataMap.get("message-type"))
                        .setSequenceNumber(Integer.parseInt(dataMap.get("sequence-number")))
                        .setSize(Double.parseDouble(dataMap.get("size")));

                if (dataMap.containsKey("imsi")) {
                    messageBuilder.setImsi(dataMap.get("imsi"));
                }
                if (dataMap.containsKey("msisdn")) {
                    messageBuilder.setMsisdn(dataMap.get("msisdn"));
                }
                if (dataMap.containsKey("teid")) {
                    messageBuilder.setTeid(dataMap.get("teid"));
                }
                if (dataMap.containsKey("cause")) {
                    messageBuilder.setCause(dataMap.get("cause"));
                }
                if (dataMap.containsKey("user-location")) {
                    messageBuilder.setUserLocation(dataMap.get("user-location"));
                }
                if (dataMap.containsKey("bearer-context")) {
                    messageBuilder.setBearerContext(dataMap.get("bearer-context"));
                }
                if (dataMap.containsKey("dpi_ip")) {
                    messageBuilder.setDpiIp(dataMap.get("dpi_ip"));
                }

                ProtMessage message = messageBuilder.build();

                System.out.println(message);
                producer.send(new ProducerRecord<>("dm_4g_gtpv2_s11", 0, Integer.valueOf(dataMap.get("sequence-number")), message));
                TimeUnit.SECONDS.sleep(2);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        producer.close();
    }

    private static String generateRandomData() {
        Random random = new Random();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
        String timestamp = dtf.format(LocalDateTime.now());
        String sourceIp = "10.202."+"" + random.nextInt(256) + "." + random.nextInt(256);
        String destIp = "10.202." + random.nextInt(256) + "." + random.nextInt(256);
        String[] messageTypes = {"Create Session Request", "Create Session Response", "Delete Session Request", "Delete Session Response", "Modify Bearer Request", "Modify Bearer Response", "Release Access Bearers Request", "Release Access Bearers Response"};
        String messageType = messageTypes[random.nextInt(messageTypes.length)];
        int sequenceNumber = random.nextInt(10000);
        String imsi = "4520488400" + (100000 + random.nextInt(900000));
        String msisdn = "84988888" + (100 + random.nextInt(900));
        double size = 5 + random.nextDouble() * 10;
        String teid = "{769184" + random.nextInt(1000) + "}";
        String cause = "Request accepted";
        String userLocation = "[Tai{tac=1001";
        String dpiIp = "10.240." + random.nextInt(256) + "." + random.nextInt(256);

        return String.format("timestamp=%s, source-ip=%s, dest-ip=%s, message-type=%s, sequence-number=%d, imsi=%s, msisdn=%s, size=%.2f, teid=%s, cause=%s, user-location=%s, dpi_ip=%s",
                timestamp, sourceIp, destIp, messageType, sequenceNumber, imsi, msisdn, size, teid, cause, userLocation, dpiIp);
    }
}