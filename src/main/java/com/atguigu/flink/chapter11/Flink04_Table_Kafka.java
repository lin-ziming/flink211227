package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink04_Table_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 直接从文件读数据
        // 读入到动态表中
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
        
        
        tEnv
            .connect(
                new Kafka()
                    .version("universal")
                    .property("bootstrap.servers", "hadoop162:9092")
                    .property("group.id", "atguigu")
                    .topic("s1")
                    .startFromLatest()
            )
            .withFormat(new Json())
            .withSchema(schema)
            .createTemporaryTable("sensor");
        
        
        // 得到一个 Table对象
        Table sensor = tEnv.from("sensor");
        
        Table result = sensor
            .where($("id").isEqual("sensor_1"))
            .select($("id"), $("ts"), $("vc"));
        
        
        // 写入到Kafka的topic中
        tEnv
            .connect(
                new Kafka()
                    .version("universal")
                    .property("bootstrap.servers", "hadoop162:9092")
                    .topic("s2")
            )
            .withFormat(new Csv().lineDelimiter(""))
            .withSchema(schema)
            .createTemporaryTable("result");
        
        result.executeInsert("result");
        
        
    }
}
