package com.atguigu.flink.chapter09;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/6/10 13:59
 */
public class Flink02_CEP_Loop {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        // 1. 现有数据流
        SingleOutputStreamOperator<WaterSensor> stream = env
            .readTextFile("input/sensor.txt")
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String value) throws Exception {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0],
                                           Long.parseLong(split[1]),
                                           Integer.parseInt(split[2])
                    );
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));
        
        
        // 2. 定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
            .<WaterSensor>begin("s1")
            .where(new SimpleCondition<WaterSensor>() {
                @Override
                public boolean filter(WaterSensor value) throws Exception {
                    return "sensor_1".equals(value.getId());
                }
            })
            //            .times(2);  // sensor_1出现2次
            //            .times(2,4);  // sensor_1出现2次或3次或4次
            .timesOrMore(1)  // 1次或多次
            //.oneOrMore()  // 等价于.timesOrMore(1)
            .until(new SimpleCondition<WaterSensor>() {  // 当次数是没有上限的时候, 一定要加终止条件
                @Override
                public boolean filter(WaterSensor value) throws Exception {
                    return "sensor_3".equals(value.getId());
                    
                }
            }); // not until
        
        // 3. 把模式作用到流, 得到把一个模式流
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);
        // 4. 从模式流中取出匹配到的数据
        SingleOutputStreamOperator<String> result = ps.select(
            new PatternSelectFunction<WaterSensor, String>() {
                // list中存储的是: 成功一次之后所有的数据
                // 每匹配成功一次, select方法就执行一次
                @Override
                public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                    return pattern.toString();
                }
            });
        
        result.print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
