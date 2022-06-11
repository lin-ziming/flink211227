package com.atguigu.flink.chapter10;

import com.atguigu.flink.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * @Author lzc
 * @Date 2022/6/8 15:20
 */
public class Flink06_High_Order_1 {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // pojo  javabean
        
        KeyedStream<OrderEvent, Long> stream = env
            .readTextFile("input/OrderLog.csv")
            .map(new MapFunction<String, OrderEvent>() {
                @Override
                public OrderEvent map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new OrderEvent(
                        Long.valueOf(data[0]),
                        data[1],
                        data[2],
                        Long.parseLong(data[3]) * 1000
                    );
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((log, ts) -> log.getEventTime())
            )
            .keyBy(OrderEvent::getOrderId);
        
        
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
            .<OrderEvent>begin("create", AfterMatchSkipStrategy.skipPastLastEvent())
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "create".equals(value.getEventType());
                }
            }).optional()
            .next("pay")
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "pay".equals(value.getEventType());
                }
            })
            .within(Time.minutes(30));
        
        PatternStream<OrderEvent> ps = CEP.pattern(stream, pattern);
        
        
        SingleOutputStreamOperator<OrderEvent> result = ps.select(
            new OutputTag<OrderEvent>("late") {},
            new PatternTimeoutFunction<OrderEvent, OrderEvent>() {
                @Override
                public OrderEvent timeout(Map<String, List<OrderEvent>> pattern,
                                          long timeoutTimestamp) throws Exception {
                    return pattern.get("create").get(0);
                }
            },
            new PatternSelectFunction<OrderEvent, OrderEvent>() {
                @Override
                public OrderEvent select(Map<String, List<OrderEvent>> pattern) throws Exception {
                    if (!pattern.containsKey("create")) {
                        return pattern.get("pay").get(0);
                    }
                    return new OrderEvent();
                }
            }
        );
    
    
        result.filter(x -> x.getOrderId() != null)
            .union(result.getSideOutput(new OutputTag<OrderEvent>("late") {}))
            .keyBy(OrderEvent::getOrderId)
            .process(new KeyedProcessFunction<Long, OrderEvent, String>() {
            
                private ValueState<OrderEvent> create;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    create = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("create", OrderEvent.class));
                }
            
                @Override
                public void processElement(OrderEvent value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if ("create".equals(value.getEventType())) {
                        out.collect(value.getOrderId() + " 只有create或者pay超时支付...");
                        create.update(value);
                    
                    } else {
                        // 判断create是否来来过, 如果来过: 证明这个pay是超时支付的. 如果没有来过, 证明是只有pay
                        if (create.value() == null) {
                            out.collect(value.getOrderId() + " 只有pay没有create...");
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
