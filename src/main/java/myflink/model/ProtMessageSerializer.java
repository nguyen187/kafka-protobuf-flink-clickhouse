package myflink.model;

import org.apache.kafka.common.serialization.Serializer;


import myflink.message.ExchangeProtoMessage.ProtMessage;

public class ProtMessageSerializer implements Serializer<ProtMessage> {
    @Override
    public byte[] serialize(String topic, ProtMessage data) {
        return data.toByteArray();
    }
}