package myflink.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


public class ConfigProperty {
    public static final String KAFKA_BROKER_LIST = "metadata.broker.list.input";
    public static final String GTPV2_S11_TOPIC= "gtpv2.s11.topic";
    public static final String GROUP_ID = "group.id";
    public static final String CHECKPOINT_PATH = "checkpoint.path.flink";


    public static final String AW_ALLOWED_LATENESS = "allowedLateness";
    public static final String AW_OOO_THRESHOLD = "oooThreshold";
    public static final String AW_SENSITIVITY = "sensitivity";
    public static final String AW_SENSITiVITY_RATE = "sensitivityChangeRate";
    public static final String AW_WINDOW_WIDTH = "windowWidth";
    public static final String AW_PERIOD = "period";


    public static final Logger LOGGER = LoggerFactory
            .getLogger(ConfigProperty.class.getName());

    public static final PropertiesConfiguration PROPERTIES = new PropertiesConfiguration();
    public static ConfigProperty instance;
    public static final String KAFKA_CONFIG_FILE = "./config/kafka.properties";
    public static final String ADAPTIVE_WATERMARK_CONFIG_FILE = "./config/adaptiveWatermark.properties";


    private PropertiesConfiguration properties = new PropertiesConfiguration();

    private ConfigProperty() {
        try (InputStream inputStream = Files.newInputStream(Paths.get(KAFKA_CONFIG_FILE))){
            properties.setDelimiterParsingDisabled(true);
            properties.load(inputStream);
            final Iterator<String> keysIter = properties.getKeys();
            while (keysIter.hasNext()) {
                String key = keysIter.next();
                System.out.println(key + "=" + properties.getString(key));
            }
        } catch (IOException | ConfigurationException e) {
            throw new RuntimeException(e);
        }


        try (InputStream inputStream = Files.newInputStream(Paths.get(ADAPTIVE_WATERMARK_CONFIG_FILE))){
            properties.setDelimiterParsingDisabled(true);
            properties.load(inputStream);
            final Iterator<String> keysIter = properties.getKeys();
            while (keysIter.hasNext()) {
                String key = keysIter.next();
                System.out.println(key + "=" + properties.getString(key));
            }
        } catch (IOException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
    public String getConfigString(String key) {
        return properties.containsKey(key) ? properties.getString(key) : null;
    }
    public Integer getConfigInt(String key) {
        return properties.containsKey(key) ? Integer.valueOf(properties.getString(key)) : null;
    }
    public Long getConfigLong(String key) {
        return properties.containsKey(key) ? Long.valueOf(properties.getString(key)) : null;
    }
    public Double getConfigDouble(String key) {
        return properties.containsKey(key) ? Double.valueOf(properties.getString(key)) : null;
    }


    public static ConfigProperty getInstance() {
        if (instance == null) {
            synchronized (ConfigProperty.class) {
                if (instance == null) {
                    instance = new ConfigProperty();
                }
            }
        }
        return instance;
    }

    static Properties loadProperties(String path){
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream(path);
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            if(input != null){
                try {
                    input.close();
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
            return prop;
        }
    }

    public Map<String, Object> createKafkaInputParameterMap(){
        LOGGER.info("Create kafka parameter map: ",KAFKA_CONFIG_FILE);
        Properties props = loadProperties(KAFKA_CONFIG_FILE);
        Map<String, Object> parameterMap = new HashMap<String, Object>();
        parameterMap.put("bootstrap.servers", props.getProperty(KAFKA_BROKER_LIST));
        parameterMap.put("group.id", props.getProperty(GROUP_ID));
        parameterMap.put("compression.type","snappy");
        parameterMap.put("enable.auto.commit", "false");
        for (Map.Entry e : props.entrySet()){
            LOGGER.info("Add kafka config ",e.getKey(),e.getValue());
            parameterMap.put((String) e.getKey(), e.getValue());
        }
        return parameterMap;
    }



}
