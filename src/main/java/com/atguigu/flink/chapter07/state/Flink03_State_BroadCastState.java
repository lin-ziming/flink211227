package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/6/7 14:17
 */
public class Flink03_State_BroadCastState {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        env.enableCheckpointing(3000);
        
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 8888);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop162", 9999);
        
        MapStateDescriptor<String, String> bcStateDesc = new MapStateDescriptor<>("bcState", String.class, String.class);
        // 1. 把控制流做成广播流
        BroadcastStream<String> bcStream = controlStream.broadcast(bcStateDesc);
        // 2. 让数据流去connect广播流, 得到一个新的流
        BroadcastConnectedStream<String, String> stream = dataStream.connect(bcStream);
        // 3. 对新流进行处理: 把广播流中数据放入的广播状态, 数据流中的数据就可以从广播状态中读取配置信息
        stream
            .process(new BroadcastProcessFunction<String, String, String>() {
                
                // 对数据流中的元素进行处理
                @Override
                public void processElement(String value,
                                           ReadOnlyContext ctx,
                                           Collector<String> out) throws Exception {
                    System.out.println("Flink03_State_BroadCastState.processElement...");
                    ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc);
    
                    String aSwitch = state.get("switch");
                    if ("1".equals(aSwitch)) {
                        out.collect("使用1号逻辑处理数据....");
                    }else if("2".equals(aSwitch)){
                        out.collect("使用2号逻辑处理数据....");
                        
                    }else{
                        out.collect("使用 默认 号逻辑处理数据....");
                        
                    }
    
    
                }
                
                // 对广播流中的元素进行处理
                @Override
                public void processBroadcastElement(String value,
                                                    Context ctx,
                                                    Collector<String> out) throws Exception {
                    System.out.println("Flink03_State_BroadCastState.processBroadcastElement....");
                    BroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc);
                    
                    state.put("switch", value);
    
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
}
