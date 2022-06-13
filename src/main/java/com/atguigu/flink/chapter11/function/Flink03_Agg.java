package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink03_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("a", 1000L, 10),
                             new WaterSensor("a", 2000L, 20),
                             new WaterSensor("b", 3000L, 30)
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式
        
        // 1.2 先注册后使用
        tEnv.createTemporaryFunction("my_avg", MyAvg.class);
        /*table
            .groupBy($("id"))
            .select($("id"), call("my_avg", $("vc")).as("vc_avg"))
            .execute()
            .print();*/
        
        // 2. 在sql中使用
        tEnv.sqlQuery("select id, my_avg(vc) from sensor group by id").execute().print();
        
    }
    
    public static class MyAvg extends AggregateFunction<Double, Avg> {
        
        //参数1: 必须是累加器  其他参数根据实际情况
        public void accumulate(Avg acc, Integer vc){
            acc.sum += vc;
            acc.count++;
        }
        
    
        // 返回最后聚合的结果
        @Override
        public Double getValue(Avg acc) {
            return acc.sum * 1.0/acc.count;
        }
        // 对累加器初始化
        @Override
        public Avg createAccumulator() {
            return new Avg();
        }
    }
    
    public static class Avg{
        public Integer sum = 0;
        public Long count = 0L;
    }
    
   
    
}
/*

 */