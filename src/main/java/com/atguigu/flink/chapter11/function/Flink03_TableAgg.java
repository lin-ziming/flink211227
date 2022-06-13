package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink03_TableAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("a", 1000L, 10),
                             new WaterSensor("a", 2000L, 20),
                             new WaterSensor("a", 2000L, 30),
                             new WaterSensor("a", 2000L, 40),
                             new WaterSensor("b", 3000L, 30)
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式
        // 1.2 先注册后使用
        tEnv.createTemporaryFunction("top_2", Top2.class);
        
        table
            .groupBy($("id"))
            .flatAggregate(call("top_2", $("vc")))
            .select($("id"), $("rank"), $("vc"))
            .execute()
            .print();
        
        // 2. 在sql中使用
        // 暂时不支持
        
    }
    
    public static class Top2 extends TableAggregateFunction<Result, FirstSecond> {
        
        // 初始化累加器
        @Override
        public FirstSecond createAccumulator() {
            return new FirstSecond();
        }
        
        // 聚合方法
        public void accumulate(FirstSecond fs, Integer vc) {
            // TODO
            if (vc > fs.first) {
                fs.second = fs.first;
                fs.first = vc;
            } else if (vc > fs.second) {
                fs.second = vc;
            }
            
        }
        
        public void emitValue(FirstSecond fs, Collector<Result> out) {
            out.collect(new Result("第一名", fs.first));
            if (fs.second > 0) {
                out.collect(new Result("第二名", fs.second));
            }
        }
        
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Result {
        
        public String rank;
        public Integer vc;
    }
    
    public static class FirstSecond {
        public Integer first = 0;
        public Integer second = 0;
    }
    
    
}
/*
每来一条数据, 输出水位的top2
10:
第一名   10

20:
第一名  20
第二名  10

30:
第一名 30
第二名 20




 */