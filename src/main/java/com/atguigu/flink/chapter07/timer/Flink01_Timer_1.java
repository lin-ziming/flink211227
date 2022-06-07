package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink01_Timer_1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        
        env.setParallelism(2);
    
        env
            .socketTextStream("hadoop162", 9999)  // 永远是1
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                }
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
    
                private long ts;
    
                @Override
                public void onTimer(long ts,  // 定时器的时间
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    // 当定时器触发的时候会自动的执行这个方法
                    System.out.println("定时器触发:" + ts);
                    out.collect(ctx.getCurrentKey() + " 水位超过20, 发出红色预警....");
                }
    
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if (value.getVc() > 20) {
                        // 注册定时器:5s之后触发的定时器
                        ts = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(ts);
                        System.out.println("注册定时器:" + ts);
                    }else if(value.getVc() < 10){
                  
                        ctx.timerService().deleteProcessingTimeTimer(ts);
                        System.out.println("删除定时器:" + ts);
                        
                    }
                }
            })
            .print();
        
        
        env.execute();
        
        
    }
}

/*
定时器: 闹钟

 */








