package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.LoginEvent;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/8 15:20
 */
public class Flink05_High_Login {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // pojo  javabean
    
        env
            .readTextFile("input/LoginLog.csv")
            .map(new MapFunction<String, LoginEvent>() {
                @Override
                public LoginEvent map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new LoginEvent(
                        Long.valueOf(data[0]),
                        data[1],
                        data[2],
                        Long.parseLong(data[3]) * 1000
                    );
                }
            })
            .keyBy(LoginEvent::getUserId)
            .countWindow(2, 1)
            .process(new ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>() {
                @Override
                public void process(Long key,
                                    Context ctx,
                                    Iterable<LoginEvent> elements,
                                    Collector<String> out) throws Exception {
                    List<LoginEvent> list = AtguiguUtil.toList(elements);
                    if (list.size() == 2) {
                        LoginEvent event1 = list.get(0);
                        LoginEvent event2 = list.get(1);
    
                        String type1 = event1.getEventType();
                        String type2 = event2.getEventType();
    
                        Long time1 = event1.getEventTime();
                        Long time2 = event2.getEventTime();
    
                        if ("fail".equals(type1) && "fail".equals(type2)  && Math.abs(time1 - time2) < 2000) {
                            out.collect("用户: " + key + "在恶意登录....");
                        }
    
                    }
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
