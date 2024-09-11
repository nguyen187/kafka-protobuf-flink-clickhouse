package com.viettel.vtnet.traffic.message;

public class TrafficMessage {
    private String sourceIp;
    private String window;
    private Double size;

    // Constructor để khởi tạo các thuộc tính
    public TrafficMessage(String sourceIp, String window, Double size) {
        this.sourceIp = sourceIp;
        this.window = window;
        this.size = size;
    }

    // Getter và Setter cho các thuộc tính
    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public String getWindow() {
        return window;
    }

    public void setWindow(String window) {
        this.window = window;
    }

    public Double getSize() {
        return size;
    }

    public void setSize(Double size) {
        this.size = size;
    }

    // Phương thức toString để in ra màn hình dữ liệu của đối tượng
    @Override
    public String toString() {
        return "SumIpMessage{" +
                "sourceIp='" + sourceIp + '\'' +
                ", window=" + window +
                ", size=" + size +
                '}';
    }
}