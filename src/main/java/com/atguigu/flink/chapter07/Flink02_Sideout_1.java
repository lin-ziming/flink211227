package com.atguigu.flink.chapter07;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink02_Sideout_1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        
        env.setParallelism(1);
    
        SingleOutputStreamOperator<WaterSensor> main = env
            .socketTextStream("hadoop162", 9999)  // 永远是1
            .map(new RichMapFunction<String, WaterSensor>() {
                @Override
                public void open(Configuration parameters) throws Exception {
                    getRuntimeContext().getState(new ValueStateDescriptor<String>("test", String.class));
                }
    
                @Override
                public WaterSensor map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
                    // 必须数据倾斜带来的水印不更新问题
                    .withIdleness(Duration.ofSeconds(5))  // 如果一个并行度水印不更新超过这个时间,则以其他并行度为准
        
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(3)))
            .sideOutputLateData(new OutputTag<WaterSensor>("late") {})  // 泛型擦除
            .sum("vc");
        
        main.print("main");
        main.getSideOutput(new OutputTag<WaterSensor>("late") {}).print("late");
    
        env.execute();
        
        
    }
}










