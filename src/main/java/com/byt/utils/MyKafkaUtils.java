package com.byt.utils;

import com.byt.deserialization.ProtoKafkaDeserialization;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

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
    public static FlinkKafkaConsumer<List<TagKafkaInfo>> getKafkaListConsumerWM(List<String> topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<List<TagKafkaInfo>> kafkaConsumer = new FlinkKafkaConsumer<List<TagKafkaInfo>>(
                topic,
                new ProtoKafkaDeserialization(),
                properties
        );
        // 分配水位线和提取时间戳
        kafkaConsumer
                // .setStartFromTimestamp(TimeUtil.getStartTime(startTime))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<List<TagKafkaInfo>>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withIdleness(Duration.ofSeconds(10L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<List<TagKafkaInfo>>() {
                                    @Override
                                    public long extractTimestamp(List<TagKafkaInfo> list, long l) {
                                        if (list.size() > 0) {
                                            return list.get(0).getTimestamp();
                                        } else {
                                            return 1606710000000L;
                                        }
                                    }
                                })
                );
        return kafkaConsumer;
    }
}
