package com.viettel.vtnet.traffic.window;

import com.viettel.vtnet.traffic.message.TrafficMessage;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
//import com.viettel.vtnet.traffic.message.;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import com.viettel.vtnet.traffic.message.ExchangeProtoMessage.ProtMessage;

public class SumSizeMessage implements WindowFunction<ProtMessage, TrafficMessage, String, TimeWindow> {
    @Override
    public void apply(String id, TimeWindow window, Iterable<ProtMessage> vals, Collector<TrafficMessage> out) {
        double sum = calculateSum(vals);
        String windowEndTime = formatWindowEndTime(window.getEnd());
        out.collect(new TrafficMessage(id, windowEndTime, sum));
    }

    private double calculateSum(Iterable<ProtMessage> vals) {
        double sum = 0.0;
        for (ProtMessage r : vals) {
            sum += r.getSize();
        }
        return sum;
    }

    private String formatWindowEndTime(long windowEnd) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.systemDefault())
                .format(Instant.ofEpochMilli(windowEnd));
    }
}