package myflink.message;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SubProtMessage {

    private String sourceIp;
    private long timestamp;
    private double size;
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    public SubProtMessage(){
        timestamp = System.currentTimeMillis();
    }

    public SubProtMessage(String sourceIp, String timestamp, double size ) throws ParseException {
        this.sourceIp = sourceIp;
        Date date = sdfDate.parse(timestamp);
        this.timestamp = date.getTime();
        this.size = size;
    }

    public long getTimestamp(){
        return timestamp;
    }

    public String getSourceIp(){
        return sourceIp;
    }

    public double getSize(){
        return size;
    }

    @Override
    public String toString(){
        return "SubProMessage("+this.sourceIp+","+this.timestamp+","+this.size+")";
    }
}
