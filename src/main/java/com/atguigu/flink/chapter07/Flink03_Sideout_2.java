package com.atguigu.flink.chapter07;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink03_Sideout_2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        
        env.setParallelism(1);
    
    
        SingleOutputStreamOperator<Integer> s1 = env
            .fromElements(10, 2, 3, 5, 8, 20, 11)
            .process(new ProcessFunction<Integer, Integer>() {
                @Override
                public void processElement(Integer value,
                                           Context ctx,
                                           Collector<Integer> out) throws Exception {
                    if (value % 2 == 0) {
                        out.collect(value);
                    }else{
                        // 把奇数放入侧输出流中
                        ctx.output(new OutputTag<Integer>("odd"){}, value);
                    }
                }
            });
    
        s1.print("偶数");
        s1.getSideOutput(new OutputTag<Integer>("odd") {}).print("奇数");
        env.execute();
        
        
    }
}










