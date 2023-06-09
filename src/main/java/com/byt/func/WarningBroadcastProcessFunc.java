package com.byt.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.pojo.TagKafkaInfo;
import com.byt.pojo.TagProperties;
import com.byt.protos.TagKafkaProtos;
import com.byt.utils.MailUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/6/9 14:17
 **/
public class WarningBroadcastProcessFunc extends BroadcastProcessFunction<Map<String,Set<String>>,String,String> {
    private MapStateDescriptor<String, List<String>> mapStateDescriptor;


    public WarningBroadcastProcessFunc(MapStateDescriptor<String, List<String>> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

    }


    @Override
    public void processElement(Map<String, Set<String>> value, BroadcastProcessFunction<Map<String, Set<String>>, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
        ReadOnlyBroadcastState<String, List<String>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        for (String key : value.keySet()) {
            List<String> confNames = broadcastState.get(key);
            System.out.println("conf:"+confNames);

            Set<String> srcNames = value.get(key);
            System.out.println("src:"+srcNames);
            if (confNames!= null){
                for (String confName : confNames) {
                    boolean flag = srcNames.contains(confName);
                    if (!flag){
                        System.out.println(confName+"没有值输入，报警！！！！");
                    }
                }
            }

        }

    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<Map<String, Set<String>>, String, String>.Context ctx, Collector<String> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String confData = jsonObject.getString("after");
        String opType = jsonObject.getString("op");
        TagProperties tagProperties = null;
        if (confData != null){
            tagProperties = JSON.parseObject(confData, TagProperties.class);
        }
        BroadcastState<String, List<String>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tagProperties.tag_topic, Arrays.asList(tagProperties.tag_name.split(",")));
    }
}
