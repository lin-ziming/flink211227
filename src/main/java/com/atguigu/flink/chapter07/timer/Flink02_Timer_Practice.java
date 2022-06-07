package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink02_Timer_Practice {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        
        env.setParallelism(1);
        
        env
            .socketTextStream("hadoop162", 9999)  // 永远是1
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            )
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                private long timerTs;
                int lastVc = 0;  // 上一次的水位
                
                @Override
                public void onTimer(long ts,  // 定时器的时间
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    
                    out.collect(ctx.getCurrentKey() + "连续5s上升, 发出预警....");
                    lastVc = 0;
                    
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    if (lastVc == 0) {  // 第一条数据过来
                        // 注册定时器
                        timerTs = value.getTs() + 5000;
                        ctx.timerService().registerEventTimeTimer(timerTs);
                        System.out.println("第一条数据过来, 注册定时器...");
                    } else {
                        //不是第一条:判断水位是否上升
                        if (value.getVc() <= lastVc) {
                            // 水位不变或者下降, 注销定时器
                            ctx.timerService().deleteEventTimeTimer(timerTs);
                            System.out.println("水位下降或者不变, 取消定时器...");
    
                            // 重新注册定时器
                            timerTs = value.getTs() + 5000;
                            ctx.timerService().registerEventTimeTimer(timerTs);
                            System.out.println("重新注册定时器...");
                            
                        } else {
                            System.out.println("水位上升, 什么都不做...");
                        }
                    }
                    
                    lastVc = value.getVc();
                }
            })
            .print();
        
        
        env.execute();
        
        
    }
}

/*
定时器: 闹钟

 */








