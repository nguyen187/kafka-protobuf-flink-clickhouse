package com.viettel.vtnet.traffic.mapper;
import org.apache.flink.api.common.functions.MapFunction;
import com.viettel.vtnet.traffic.message.ExchangeProtoMessage.ProtMessage;
public class IpMapper implements MapFunction<ProtMessage, ProtMessage> {
    @Override
    public ProtMessage map(ProtMessage r) {
        String newSourceIp = extractSubnet(r.getSourceIp());
        return new ProtMessage(newSourceIp, r.getTimestamp(), r.getSize());
    }

    private String extractSubnet(String ip) {
        String[] parts = ip.split("\\.");
        return parts[0] + "." + parts[1];
    }
}