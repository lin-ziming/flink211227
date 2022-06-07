package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/6/7 14:17
 */
public class Flink01_State_Operator_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        env.enableCheckpointing(3000);
        
        
        env
            .socketTextStream("hadoop162", 9999)
            .flatMap(new MyFlatMapFunction())
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MyFlatMapFunction implements FlatMapFunction<String, String>, CheckpointedFunction {
        
        ArrayList<String> list = new ArrayList<>();
        private ListState<String> listState;
    
        @Override
        public void flatMap(String line,
                            Collector<String> out) throws Exception {
    
            if (line.contains("x")) {
                throw new RuntimeException("主动抛异常, 程序自动重启");
            }
            
            String[] words = line.split(" ");
    
            for (String word : words) {
                list.add(word);
            }
            
            out.collect(list.toString());
        }
        
        // 做快照: 周期性的执行,执行的次数和并行度相关
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
//            System.out.println("MyFlatMapFunction.snapshotState");
            
            /*listState.clear();
            listState.addAll(list);*/
            listState.update(list);
            
            
        }
        
        // 初始化状态: 程序 启动的时候执行一次. 可以在这里把状态中保存的值进行恢复
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            System.out.println("MyFlatMapFunction.initializeState");
            // alt+ctrl+f
            listState = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<String>("words", String.class));
            
            // 从状态中读取数据, 恢复到ArrayList中
            Iterable<String> it = listState.get();
    
            for (String word : it) {
                System.out.println("恢复状态....");
                list.add(word);
                
            }
    
        }
    }
}
