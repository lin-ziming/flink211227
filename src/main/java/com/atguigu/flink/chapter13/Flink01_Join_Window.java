package com.atguigu.flink.chapter13;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author lzc
 * @Date 2022/6/14 10:12
 */
public class Flink01_Join_Window {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        SingleOutputStreamOperator<WaterSensor> s1 = env
            .socketTextStream("hadoop162", 8888)  // 在socket终端只输入毫秒级别的时间戳
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                        @Override
                        public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                            return element.getTs() * 1000;
                        }
                    })
            );
        
        SingleOutputStreamOperator<WaterSensor> s2 = env
            .socketTextStream("hadoop162", 9999)  // 在socket终端只输入毫秒级别的时间戳
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                        @Override
                        public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                            return element.getTs() * 1000;
                        }
                    })
            );
    
    
        s1
            .join(s2)
            .where(WaterSensor::getId)
            .equalTo(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new JoinFunction<WaterSensor, WaterSensor, String>() {
                @Override
                public String join(WaterSensor first,
                                   WaterSensor second) throws Exception {
                    return first + "<>" + second;
                }
            })
            .print();
        
        
        
        env.execute();
        
        
    }
}
