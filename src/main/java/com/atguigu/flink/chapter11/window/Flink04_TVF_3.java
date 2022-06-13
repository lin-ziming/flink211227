package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink04_TVF_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Person> stream =
            env
                .fromElements(
                    new Person("a", "b1", "c1", 1000L, 10),
                    new Person("a", "b1", "c2", 2000L, 20),
                    new Person("a", "b2", "c1", 3000L, 30),
                    new Person("a", "b2", "c2", 4000L, 10),
                    new Person("a", "b1", "c1", 5000L, 50)
                )
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<Person>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
                );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(stream, $("a"), $("b"), $("c"), $("ts").rowtime(), $("score"));
        
        tEnv.createTemporaryView("person", table);
        
        
        // select .. from t group a,b,c;
        // select .. from t group a,b
        
        
       /* tEnv
            .sqlQuery("select " +
                          "a,b,c, window_start, window_end, " +
                          "sum(score) sum_score " +
                          "from table(tumble(table person, descriptor(ts), interval '5' second)) group by a,b,c, window_start, window_end " +
                          "union " +
                          "select " +
                          "a,b,'test', window_start, window_end, " +
                          "sum(score) sum_score " +
                          "from table(tumble(table person, descriptor(ts), interval '5' second)) group by a,b, window_start, window_end "
            )
            .execute()
            .print();*/
    
        /*tEnv
            .sqlQuery("select " +
                          "a,b,c, window_start, window_end, " +
                          "sum(score) sum_score " +
                          "from table(tumble(table person, descriptor(ts), interval '5' second)) " +
                          "group by window_start, window_end, grouping sets( (a,b,c), (a,b),(a),()  ) "
            )
            .execute()
            .print();*/
    
        /*tEnv
            .sqlQuery("select " +
                          "a,b,c, window_start, window_end, " +
                          "sum(score) sum_score " +
                          "from table(tumble(table person, descriptor(ts), interval '5' second)) " +
                          "group by window_start, window_end, rollup(a,b,c) "  // 是它的简化:grouping sets( (a,b,c), (a,b),(a),()
            )
            .execute()
            .print();*/
    
    
        
        /*
        // 是它的简化:grouping sets(
          (a,b,c),
         (a,b),(a,c),(b,c),
         (a),(b),(c)
         ()
         */
        tEnv
            .sqlQuery("select " +
                          "a,b,c, window_start, window_end, " +
                          "sum(score) sum_score " +
                          "from table(tumble(table person, descriptor(ts), interval '5' second)) " +
                          "group by window_start, window_end, cube(a,b,c) "
            )
            .execute()
            .print();
        
        
    }
}
/*
每隔一小时统计一次今天的 pv 值

流:
    定时器
    
    窗口
        状态
        
sql:
    累积窗口



 */