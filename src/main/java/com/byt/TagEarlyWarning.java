package com.byt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.cdc.FlinkCDC;
import com.byt.func.SendMailAsyncFunction;
import com.byt.func.SendProcessFunction;
import com.byt.func.WarningBroadcastProcessFunc;
import com.byt.pojo.TagKafkaInfo;
import com.byt.pojo.TagProperties;
import com.byt.utils.ConfigManager;
import com.byt.utils.MailUtil;
import com.byt.utils.MyKafkaUtils;
import com.byt.utils.ThreadPoolUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import javax.mail.MessagingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/6/9 11:14
 **/
public class TagEarlyWarning {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取kafka标签数据
        SingleOutputStreamOperator<Map<String, Set<String>>> kafkaSource = env
                .addSource(MyKafkaUtils.getKafkaListConsumer(ConfigManager.getListProperty("kafka.ods.topic"), ConfigManager.getProperty("kafka.group.id")))
                .flatMap(new RichFlatMapFunction<List<TagKafkaInfo>, TagKafkaInfo>() {
                    @Override
                    public void flatMap(List<TagKafkaInfo> value, Collector<TagKafkaInfo> out) throws Exception {
                        for (TagKafkaInfo tagKafkaInfo : value) {
                            out.collect(tagKafkaInfo);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withIdleness(Duration.ofSeconds(10L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TagKafkaInfo>() {
                                    @Override
                                    public long extractTimestamp(TagKafkaInfo element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })

                )
                .keyBy(r -> r.getTopic())
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .process(new ProcessWindowFunction<TagKafkaInfo, Map<String, Set<String>>, String, TimeWindow>() {

                    private MapState<String, Set<String>> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<String, Set<String>>(
                                        "mapstate",
                                        Types.STRING,
                                        TypeInformation.of(new TypeHint<Set<String>>() {
                                        })));
                    }

                    @Override
                    public void process(String s, ProcessWindowFunction<TagKafkaInfo, Map<String, Set<String>>, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<Map<String, Set<String>>> out) throws Exception {
                        HashSet<String> set = new HashSet<>();
                        for (TagKafkaInfo element : elements) {
                            set.add(element.getName());
                        }
                        if (!mapState.contains(s)) {
                            mapState.put(s, set);
                        } else {
                            mapState.get(s).addAll(set);
                        }
                        HashMap<String, Set<String>> stringSetHashMap = new HashMap<>();
                        stringSetHashMap.put(s, mapState.get(s));
                        out.collect(stringSetHashMap);
                        mapState.remove(s);
                        set.clear();
                    }
                });


        // 定义广播状态描述器、读取配置流转换为广播流
//        MapStateDescriptor<String, List<String>> mapStateDescriptor = new MapStateDescriptor<>(
//                "map-state",
//                Types.STRING,
//                Types.LIST(Types.STRING)
//        );
        MapStateDescriptor<String, TagProperties> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                Types.STRING,
                Types.POJO(TagProperties.class)
        );

        // 定义测输出流 收集报警信息
        OutputTag<Tuple2<String, String>> warningMsgTag = new OutputTag<Tuple2<String, String>>("warningMsg") {
        };

        // cdc读取配置数据
        BroadcastStream<String> mysqlCdcSource = env
                .fromSource(FlinkCDC.getMysqlSource(), WatermarkStrategy.noWatermarks(), "mysql-cdc")
                .broadcast(mapStateDescriptor);

        // 连接两个流
        SingleOutputStreamOperator<String> broadcastDs = kafkaSource
                .connect(mysqlCdcSource)
                .process(new WarningBroadcastProcessFunc(mapStateDescriptor, warningMsgTag));
        SingleOutputStreamOperator<String> warningDS = broadcastDs
                .getSideOutput(warningMsgTag)
                .keyBy(r -> r.f0)
                .process(new SendProcessFunction());

        warningDS.print("------>");
        // 异步告警
        AsyncDataStream
                .orderedWait(warningDS,
                        new SendMailAsyncFunction<String>() {
                            @Override
                            public String getMsg(String input) {
                                return input;
                            }
                        },
                        1,
                        TimeUnit.HOURS
                );

        env.execute("TagEarlyWarningJob");
    }
}
