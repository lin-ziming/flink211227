package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink03_Table_File {
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
            .connect(new FileSystem().path("input/sensor.txt"))
            .withFormat(new Csv())
            .withSchema(schema)
            .createTemporaryTable("sensor");
        
        // 得到一个 Table对象
        Table sensor = tEnv.from("sensor");
        
        Table result = sensor
            .where($("id").isEqual("sensor_1"))
            .select($("id"), $("ts"), $("vc"));
        
        // 把result写入到文件中
        
        // 建立一个动态表, 与输出文件关联
        tEnv
            .connect(new FileSystem().path("input/a.txt"))
            .withFormat(new Csv())
            .withSchema(schema)
            .createTemporaryTable("result");
        
        
        result.executeInsert("result");
    
    
    }
}
