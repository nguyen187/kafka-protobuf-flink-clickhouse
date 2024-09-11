package com.viettel.vtnet.traffic.consumer;

public class KafkaConsumerWithProto {

    public static void main(String[] args) {

//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
//        props.setProperty("group.id", "test");
//        props.setProperty("enable.auto.commit", "true");
//        props.setProperty("auto.commit.interval.ms", "1000");
//
////        KafkaConsumer<Integer, ProtMessage> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new ProtMessageDeserializer());
//        consumer.subscribe(Collections.singletonList("datamon"));
//
//        while (true) {
//
//            ConsumerRecords<Integer, ProtMessage> records = consumer.poll(Duration.ofMillis(100));
//
//            for (ConsumerRecord<Integer, ProtMessage> record : records) {
//                System.out.println("Received message: (" + record.key() + ", " + record.value().toString() + ") at offset " + record.offset());
//            }
//        }
    }
}