package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/8 15:20
 */
public class Flink04_High_Ads {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // pojo  javabean
        
        env
            .readTextFile("input/AdClickLog.csv")
            .map(new MapFunction<String, AdsClickLog>() {
                @Override
                public AdsClickLog map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new AdsClickLog(
                        Long.valueOf(data[0]),
                        Long.valueOf(data[1]),
                        data[2],
                        data[3],
                        Long.parseLong(data[4]) * 1000
                    );
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((log, ts) -> log.getTimestamp())
            )
            .keyBy(log -> log.getUserId() + "_" + log.getAdsId())
            .process(new KeyedProcessFunction<String, AdsClickLog, String>() {
    
                private ValueState<String> yesterdayState;
                private ValueState<Boolean> alreadyBlackListState;
                private ReducingState<Long> clickCountState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    clickCountState = getRuntimeContext().getReducingState(
                        new ReducingStateDescriptor<Long>("clickCountState", Long::sum, Long.class));
                    
                    alreadyBlackListState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("alreadyBlackListState", Boolean.class));
    
                    yesterdayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("yesterdayState", String.class));
                }
                
                @Override
                public void processElement(AdsClickLog value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    // 如何知道数据变成了第二天的数据: 用状态记录年月日, 如果发生变化了,则变成了第二天
                    String yesterday = yesterdayState.value();
                    String today = new SimpleDateFormat("yyyy-MM-dd").format(value.getTimestamp());
                    if (!today.equals(yesterday)) {  //跨天了, 则清除状态
                        System.out.println(today);
                        yesterdayState.update(today);
                        
                        clickCountState.clear();
                        alreadyBlackListState.clear();
                    }
    
    
                    if (alreadyBlackListState.value() == null) {  // 没有加入黑名单, 才进行累计
                        clickCountState.add(1L);
                    }
                    
                    
                    Long count = clickCountState.get();
                    String msg = "用户: " + value.getUserId() + " 对广告" + value.getAdsId() + "的点击量是:" + count;
                    
                    if (count > 99) {
                        if (alreadyBlackListState.value() == null) {
                            msg += " 超过阈值, 加入黑名单";
                            alreadyBlackListState.update(true);
                            out.collect(msg);
                        }
                    } else {
                        out.collect(msg);
                    }
                    
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
    }
}
