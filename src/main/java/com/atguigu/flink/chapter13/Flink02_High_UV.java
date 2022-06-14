package com.atguigu.flink.chapter13;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/8 15:20
 */
public class Flink02_High_UV {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // pojo  javabean
        
        env
            .readTextFile("input/UserBehavior.csv")
            .map(new MapFunction<String, UserBehavior>() {
                @Override
                public UserBehavior map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new UserBehavior(
                        Long.valueOf(data[0]),
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2]),
                        data[3],
                        Long.parseLong(data[4]) * 1000
                    );
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ub, ts) -> ub.getTimestamp())
            )
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .keyBy(UserBehavior::getBehavior)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
                
                
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<UserBehavior> elements,
                                    Collector<String> out) throws Exception {
                    BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), 100 * 10000, 0.01);
    
                    long sum = 0;
                    for (UserBehavior element : elements) {
                        if (bf.put(element.getUserId())) {
                            sum++;
                        }
                    }
                    
                    out.collect(ctx.window() + "   " + sum);
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
