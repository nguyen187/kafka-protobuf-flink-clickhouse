package myflink.model;


import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import myflink.message.ExchangeProtoMessage.ProtMessage;

import java.io.IOException;

public class ProtMessageDeserializer extends AbstractDeserializationSchema<ProtMessage> {
//    @Override
//    public ProtMessage deserialize(String topic, byte[] data) {
//        try {
//            return ProtMessage.parseFrom(data);
//        } catch (InvalidProtocolBufferException e) {
//            e.printStackTrace();
//            throw new RuntimeException("excepiton while parsing");
//        }
//    }

    @Override
    public ProtMessage deserialize(byte[] bytes) throws IOException {
        try {
            return ProtMessage.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new RuntimeException("excepiton while parsing");
        }
    }
}


