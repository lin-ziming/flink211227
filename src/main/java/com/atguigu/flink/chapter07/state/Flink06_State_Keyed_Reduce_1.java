package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink06_State_Keyed_Reduce_1 {
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
    
    
                private ReducingState<WaterSensor> vcSumState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    vcSumState = getRuntimeContext().getReducingState(
                        new ReducingStateDescriptor<WaterSensor>("vcSumState",
                                                                 new ReduceFunction<WaterSensor>() {
                                                                     @Override
                                                                     public WaterSensor reduce(WaterSensor value1,
                                                                                               WaterSensor value2) throws Exception {
                                                                         value1.setVc(value1.getVc() + value2.getVc());
                                                                         return value1;
                                                                     }
                                                                 },
                                                                 WaterSensor.class
                        ));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    
                    vcSumState.add(value);
                    
                    out.collect(ctx.getCurrentKey() + "    " + vcSumState.get().getVc());
                }
                
            })
            .print();
        
        
        env.execute();
        
        
    }
}










