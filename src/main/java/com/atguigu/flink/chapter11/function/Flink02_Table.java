package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink02_Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("aa bb cccc", 1000L, 10),
                             new WaterSensor("hello atguigu aaa", 2000L, 20),
                             new WaterSensor("a b c", 3000L, 30)
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式
        
        // 1.2 先注册后使用
        tEnv.createTemporaryFunction("my_split", MySplit.class);
        table
            .joinLateral(call("my_split", $("id")))
            .select($("id"), $("word"), $("len"))
            .execute()
            .print();
        
        
        
        // 2. 在sql中使用
        
        
        
        
    }
    
    @FunctionHint(output = @DataTypeHint("row<word string, len int>"))
    public static class MySplit extends TableFunction<Row> {
        public void eval(String s){
            if (s == null) {
                return;
            }
            String[] words = s.split(" ");
            for (String word : words) {
                collect(Row.of(word, word.length()));
            }
        }
    }
    
}
/*
"hello world atguigu":
    hello   5
    world   5
    atguigu 7
    
"aa b c":
    aa 2
    b  1
    c  1
    
--------
"hello world atguigu"   hello   5
"hello world atguigu"   world   5
"hello world atguigu"   atguigu   7

....

-----
制出来的表是如何与原表连在一起的?
  join

 */