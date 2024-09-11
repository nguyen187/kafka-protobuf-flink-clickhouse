package com.viettel.vtnet.traffic.clickhouse;

import com.viettel.vtnet.traffic.message.TrafficMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class ClickHouseSink extends RichSinkFunction<TrafficMessage> {
    private transient Connection connection;
    private transient PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        String url = "jdbc:clickhouse://localhost:8123/default";

        Properties properties = new Properties();
        properties.setProperty("user", "default");
        properties.setProperty("password", "default");

        connection = DriverManager.getConnection(url, properties);
        String sql = "INSERT INTO sum_ip_message (sourceIp, window, size) VALUES (?, ?, ?)";
        statement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(TrafficMessage value, Context context) throws Exception {
        // Set parameters and execute update
        statement.setString(1, value.getSourceIp());
        statement.setString(2, value.getWindow());
        statement.setDouble(3, value.getSize());
        statement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
