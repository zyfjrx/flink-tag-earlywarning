package com.byt.mock;

import com.byt.deserialization.TopicDataDeserialization;
import com.byt.utils.ConfigManager;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @title: kafka工具类
 * @author: zhang
 * @date: 2022/8/17 14:27
 */
public class MyKafkaUtilMock {


    private static String defaultTopic = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();

    static {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getProperty("kafka.server"));

    }

    /**
     * 回流ods生产者
     *
     * @return
     */
    public static FlinkKafkaProducer getProducerWithTopicData() {
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000L + "");
        return new FlinkKafkaProducer<TopicData>(
                defaultTopic,
                new TopicDataDeserialization(),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }



}
