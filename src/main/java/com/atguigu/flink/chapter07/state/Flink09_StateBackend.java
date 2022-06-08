package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/7 9:40
 */
public class Flink09_StateBackend {
    public static void main(String[] args) throws Exception {
    
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        
        env.setParallelism(2);
        
        
        env.enableCheckpointing(3000); // 开启checkpoint
        
        // 1. 内存
        // 1.1 旧
//        env.setStateBackend(new MemoryStateBackend());  // 本地在内存, 远程也在内存
        // 1.2新
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        
        // 2. fs
        // 2.1 旧
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/ck1")); // 本地在内存, 远程在hdfs
        // 2.2
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck2");
        // 3. rocksdb
        
        // 3.1 Old
//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/ck3"));
        // 3.2 new
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck4");
        
        
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
    
    
                private MapState<Integer, Object> vcsState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    vcsState = getRuntimeContext().getMapState(
                        new MapStateDescriptor<Integer, Object>("vcsState", Integer.class, Object.class));
                    
        
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    vcsState.put(value.getVc(),new Object());
    
                    Iterable<Integer> it = vcsState.keys();
                    List<Integer> keys = AtguiguUtil.toList(it);
                    
                    out.collect(ctx.getCurrentKey() + "   " + keys);
                }
                
            })
            .print();
        
        
        env.execute();
        
        
    }
    
 
}










