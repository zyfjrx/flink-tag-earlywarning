package com.byt;

import com.byt.cdc.FlinkCDC;
import com.byt.func.WarningBroadcastProcessFunc;
import com.byt.pojo.TagKafkaInfo;
import com.byt.pojo.TagProperties;
import com.byt.utils.ConfigManager;
import com.byt.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

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
                .addSource(MyKafkaUtils.getKafkaListConsumerWM(ConfigManager.getListProperty("kafka.ods.topic"), ConfigManager.getProperty("kafka.group.id")))
                .flatMap(new RichFlatMapFunction<List<TagKafkaInfo>, TagKafkaInfo>() {
                    @Override
                    public void flatMap(List<TagKafkaInfo> value, Collector<TagKafkaInfo> out) throws Exception {
                        for (TagKafkaInfo tagKafkaInfo : value) {
                            out.collect(tagKafkaInfo);
                        }
                    }
                })
                .keyBy(r -> r.getTopic())
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .process(new ProcessWindowFunction<TagKafkaInfo, Map<String, Set<String>>, String, TimeWindow>() {

                    private MapState<String,Set<String>> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<String,Set<String>>(
                                "mapstate",
                                Types.STRING,
                                TypeInformation.of(new TypeHint<Set<String>>(){})));
                    }

                    @Override
                    public void process(String s, ProcessWindowFunction<TagKafkaInfo, Map<String, Set<String>>, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<Map<String, Set<String>>> out) throws Exception {
                        HashSet<String> set = new HashSet<>();
                        for (TagKafkaInfo element : elements) {
                           set.add(element.getName());
                        }
                        if (!mapState.contains(s)) {
                            mapState.put(s,set);
                        }else {
                            mapState.get(s).addAll(set);
                        }
                        HashMap<String, Set<String>> stringSetHashMap = new HashMap<>();
                        stringSetHashMap.put(s,mapState.get(s));
                        out.collect(stringSetHashMap);
                        mapState.remove(s);
                        set.clear();
                    }
                });

        // 定义广播状态描述器、读取配置流转换为广播流
        MapStateDescriptor<String, List<String>> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                Types.STRING,
                Types.LIST(Types.STRING)
        );
        // cdc读取配置数据
        BroadcastStream<String> mysqlCdcSource = env
                .fromSource(FlinkCDC.getMysqlSource(), WatermarkStrategy.noWatermarks(), "mysql-cdc")
                .broadcast(mapStateDescriptor);

        // 连接两个流
        kafkaSource
                .connect(mysqlCdcSource)
                .process(new WarningBroadcastProcessFunc(mapStateDescriptor))
                .setParallelism(1);


        env.execute("TagEarlyWarningJob");
    }
}
