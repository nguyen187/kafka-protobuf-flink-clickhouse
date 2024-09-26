package myflink.source;

import myflink.message.ExchangeProtoMessage;


import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import watermarkModule.AdaptiveWatermarkEstimator;
import watermarkModule.WatermarkEstimator;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AdaptiveWatermarkAssigner implements WatermarkGenerator<ExchangeProtoMessage.ProtMessage> {
    private long watermarkPeriod;
    private long maxTimestamp = Long.MIN_VALUE;
    private long maxAllowedLateness;

    private double OOOThreshold;
    private double sensitivity;
    private long totalOOOArrival=0;
    private long totalElements =0;
    long currentWatermark=0;

    WatermarkEstimator estimator;
    protected long numberOfGeneratedWatermarks=0;
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");


    public long getNumberOfGeneratedWatermarks(){return numberOfGeneratedWatermarks;}

    public AdaptiveWatermarkAssigner()
    {
        watermarkPeriod = 1000;
        maxAllowedLateness = 1000;
        OOOThreshold = 1.1;
        sensitivity = 1.0;
        estimator = new AdaptiveWatermarkEstimator();
    }

    public AdaptiveWatermarkAssigner(long maxLateness, double oooThrshld, double sns)
    {
        watermarkPeriod = 1000;
        maxAllowedLateness = maxLateness;
        OOOThreshold = oooThrshld;
        sensitivity = sns;
        estimator = new AdaptiveWatermarkEstimator(sns, 1.0, oooThrshld, maxLateness);
    }

    public AdaptiveWatermarkAssigner(long watermarkPeriod, long maxAllowedLateness)
    {
        this.watermarkPeriod = watermarkPeriod;
        this.maxAllowedLateness = maxAllowedLateness;
        OOOThreshold = 1.1;
        sensitivity = 1.0;
        //estimator = new PeriodicWaterMarkEstimator(watermarkPeriod, maxAllowedLateness);
        estimator = new AdaptiveWatermarkEstimator(sensitivity, 1.0,OOOThreshold,maxAllowedLateness);
    }

    public AdaptiveWatermarkAssigner(long maxAllowedLateness, double oooThreshold, double sensitivity, double sensitivityChangeRate)
    {
        this.maxAllowedLateness = 1000;

        estimator = new AdaptiveWatermarkEstimator(sensitivity,sensitivityChangeRate,oooThreshold,maxAllowedLateness);
    }

//    @Override
//    public long extractTimestamp(ExchangeProtoMessage.ProtMessage element, long previousElementTimestamp) {
//        try {
//            Date date = sdfDate.parse(element.getTimestamp());
//            long timestamp = date.getTime();
//            currentTimestamp = timestamp;
//            return timestamp;
//        } catch (ParseException e) {
//            e.printStackTrace();
//            return currentTimestamp;
//        }
//    }

    @Override
    public void onEvent(ExchangeProtoMessage.ProtMessage protMessage, long extractedTimestamp, WatermarkOutput watermarkOutput) {
        totalElements++;
        if (estimator.processEvent(extractedTimestamp, System.currentTimeMillis())) {
            // Generate a new watermark if the conditions are met
            if (estimator.getWatermark() > currentWatermark) {
                currentWatermark = estimator.getWatermark();
                Watermark wm = new Watermark(currentWatermark);
                watermarkOutput.emitWatermark(wm);

                numberOfGeneratedWatermarks++;
                System.out.println("Generating a new watermark with timestamp (" + wm.getTimestamp() + ") " +
                        sdfDate.format(new Date(wm.getTimestamp())));
                System.out.println("Total number of generated watermarks: " + numberOfGeneratedWatermarks);
            }
        }

        // Check for late arrivals
        if (estimator.wasLastElementALateArrival()) {
            System.out.println("Late arrival detected for event with timestamp: " + extractedTimestamp);
            totalOOOArrival++;
        }
//        System.out.println("Total OOO Arrival "+totalOOOArrival +" of total elements "+totalElements +" with percentage "+(double)totalOOOArrival/totalElements);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

    }


//    @Nullable
//    @Override
//    public Watermark checkAndGetNextWatermark(ExchangeProtoMessage.ProtMessage protMessage, long extractedTimestamp) {
//        if (estimator.processEvent(extractedTimestamp, System.currentTimeMillis()))
//        {
//            // We need to generate the watermark
//            //      if (estimator.getWatermark() > currentWatermark && currentWatermark != 0)
//            //       System.out.println("Pending windows "+((estimator.getWatermark() - currentWatermark)/100));
//
//            if (estimator.getWatermark() > currentWatermark) {
//                numberOfGeneratedWatermarks++;
//                currentWatermark = estimator.getWatermark();
//                Watermark wm = new Watermark(estimator.getWatermark());
//                System.out.println("Generating a new watermark with timestamp ("+wm.getTimestamp()+")" + sdfDate.format(new Date(wm.getTimestamp())));
//                System.out.println("Total number of generated watermarks "+this.getNumberOfGeneratedWatermarks());
//                return new Watermark(wm.getTimestamp());
//
//            }
//        }
//        if (estimator.wasLastElementALateArrival())
//        {
////            System.out.println("\t Arrival of an event "+ se.toString()+" behind the watermark "+estimator.getWatermark());
//            totalOOOArrival++;
//        }
//        return null;
//    }
}