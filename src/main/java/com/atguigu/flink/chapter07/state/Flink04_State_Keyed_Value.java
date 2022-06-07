package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink04_State_Keyed_Value {
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
    
                private ValueState<Integer> lastVcState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class));
                }
    
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer lastVc = lastVcState.value();
    
                    if (lastVc != null) {
                        // 不是第一个数据
                        if (value.getVc() > 10 && lastVc > 10) {
                            out.collect(ctx.getCurrentKey() + "  连续两次水位超过10, 发出预警...");
                        }
                    }
                    
                    lastVcState.update(value.getVc());
                    
                }
            })
            .print();
           
        
        env.execute();
        
        
    }
}










