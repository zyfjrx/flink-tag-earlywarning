package com.byt.utils;

import com.byt.deserialization.ProtoKafkaDeserialization;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @title: kafka 工具类
 * @author: zhangyf
 * @date: 2023/6/9 11:05
 **/
public class MyKafkaUtils {

    private static String defaultTopic = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();

    static {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getProperty("kafka.server"));
    }
    /**
     * kafka-消费者 事件时间
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<List<TagKafkaInfo>> getKafkaListConsumer(List<String> topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<List<TagKafkaInfo>> kafkaConsumer = new FlinkKafkaConsumer<List<TagKafkaInfo>>(
                topic,
                new ProtoKafkaDeserialization(),
                properties
        );
        // 分配水位线和提取时间戳
        return kafkaConsumer;
    }


    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(
                defaultTopic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(MyKafkaUtils.getKafkaListConsumer(ConfigManager.getListProperty("kafka.ods.topic"), ConfigManager.getProperty("kafka.group.id")))
                .print();

        env.execute();
    }
}
