package com.viettel.vtnet.traffic.model;

import org.apache.kafka.common.serialization.Serializer;
import com.viettel.vtnet.traffic.message.ExchangeProtoMessage.ProtMessage;

public class ProtMessageSerializer implements Serializer<ProtMessage> {
    @Override
    public byte[] serialize(String topic, ProtMessage data) {
        return data.toByteArray();
    }
}