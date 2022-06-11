package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink02_Table_BaseUse_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
        
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 1. 获得一个动态表: 把一个流转成动态表
        Table table = tEnv.fromDataStream(stream);
        
        // select id, sum(vc) as vc_sum from t group by id
        Table result = table
            .groupBy($("id"))
            .aggregate($("vc").sum().as("vc_sum"))
            .select($("id"), $("vc_sum"));
    
        // 3. 把动态表转成流, 最后输出
        /*DataStream<Tuple2<Boolean, Row>> resultStream = tEnv.toRetractStream(result, Row.class).filter(t ->t.f0);
        resultStream.map(t -> t.f1).print();
    */
        result.execute().print();
    
//        env.execute();
    
    
    }
}
