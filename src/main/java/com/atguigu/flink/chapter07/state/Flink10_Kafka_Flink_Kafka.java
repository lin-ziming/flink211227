package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink10_Kafka_Flink_Kafka {
    public static void main(String[] args) throws Exception {
    
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        
        env.setParallelism(2);
        
        // 1. 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck10");
        // 2. 开启checkpoint
        env.enableCheckpointing(3000);
        
        // 3. 设置一致性语义
        env.getCheckpointConfig().setCheckpointingMode(EXACTLY_ONCE);
        
        // 4. 其他的一些高级设置
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);  //  同时并发的checkpoint的数量
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // 两个checkpoint之间的最小时间间隔
        env.getCheckpointConfig().setCheckpointTimeout(60 *1000);  // checkpoint 超时时间
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
    
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("hadoop162:9092,hadoop163:9092,hadoop164:9092")
            .setGroupId("atguigu3")
            .setTopics("s1")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setStartingOffsets(OffsetsInitializer.latest())
            .build();
    
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source")
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String line,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : line.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
            })
            .keyBy(t -> t.f0)
            .sum(1);
    
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.put("transaction.timeout.ms", 15 * 60 * 1000);
        resultStream.addSink(new FlinkKafkaProducer<Tuple2<String, Long>>(
            "default",
            new KafkaSerializationSchema<Tuple2<String, Long>>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> t,
                                                                @Nullable Long timestamp) {
                    return new ProducerRecord<>("s2", (t.f0 + "_" + t.f1).getBytes(StandardCharsets.UTF_8));
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));
    
        
        resultStream.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                if (value.f0.contains("x")) {
                    throw new RuntimeException("xxxx");
                }
            }
        });
    
        env.execute();
        
        
    }
    
 
}










