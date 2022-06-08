package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink07_State_Keyed_Aggregate {
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
                
                
                private AggregatingState<WaterSensor, Double> avgState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    avgState = getRuntimeContext().getAggregatingState(
                        new AggregatingStateDescriptor<WaterSensor, Avg, Double>(
                            "avgState",
                            new AggregateFunction<WaterSensor, Avg, Double>() {
                                @Override
                                public Avg createAccumulator() {
                                    return new Avg();
                                }
                                
                                @Override
                                public Avg add(WaterSensor value, Avg acc) {
                                    acc.sum += value.getVc();
                                    acc.count++;
                                    return acc;
                                }
                                
                                @Override
                                public Double getResult(Avg acc) {
                                    return acc.sum * 1.0 / acc.count;
                                }
                                
                                @Override
                                public Avg merge(Avg a, Avg b) {
                                        return null;
                                }
                            },
                            Avg.class
                        ));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    avgState.add(value);
                    out.collect(ctx.getCurrentKey() + "    " + avgState.get());
                }
                
            })
            .print();
        
        
        env.execute();
        
        
    }
    
    public static class Avg {
        public Integer sum = 0;
        public Long count = 0L;
    }
}










