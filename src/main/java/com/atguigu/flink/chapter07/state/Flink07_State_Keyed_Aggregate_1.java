package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink07_State_Keyed_Aggregate_1 {
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
                        new AggregatingStateDescriptor<WaterSensor, Tuple2<Integer, Long>, Double>(
                            "avgState",
                            new AggregateFunction<WaterSensor, Tuple2<Integer, Long>, Double>() {
                                @Override
                                public Tuple2<Integer, Long> createAccumulator() {
                                    return Tuple2.of(0,0L);
                                }
            
                                @Override
                                public Tuple2<Integer, Long> add(WaterSensor value, Tuple2<Integer, Long> acc) {
                                    return Tuple2.of(acc.f0 + value.getVc(), acc.f1 + 1);
                                }
            
                                @Override
                                public Double getResult(Tuple2<Integer, Long> acc) {
                                    return acc.f0 *1.0 / acc.f1;
                                }
            
                                @Override
                                public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> a, Tuple2<Integer, Long> b) {
                                    return null;
                                }
                            },
//                            TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {})
                            Types.TUPLE(Types.INT, Types.LONG)
                        )

                    );
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
    
 
}










