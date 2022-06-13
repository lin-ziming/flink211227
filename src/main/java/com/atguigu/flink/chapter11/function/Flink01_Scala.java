package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink01_Scala {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60)
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式
        /*table
            .select($("id"), call(MyUpperCase.class, $("id")))
            .execute()
            .print();*/
        
        // 1.2 先注册后使用
      /*  tEnv.createTemporaryFunction("my_upper", MyUpperCase.class);
        table
            .select($("id"), call("my_upper", $("id")))
            .execute()
            .print();*/
        
        // 2. 在sql中使用
        
        tEnv.createTemporaryFunction("my_upper", MyUpperCase.class);
        tEnv
            .sqlQuery("select  " +
                          " id, my_upper(id) " +
                          "from sensor")
            .execute()
            .print();
        
        
        
        
    }
    
    public static class MyUpperCase extends ScalarFunction {
        public String eval(String s) {
            if (s == null) {
                return null;
            }
            return s.toUpperCase();
        }
    }
    
    
}
