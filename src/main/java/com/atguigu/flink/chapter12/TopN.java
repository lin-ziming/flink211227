package com.atguigu.flink.chapter12;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/14 8:59
 */
public class TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 1. 建立动态表, 与文件关联
        tEnv.executeSql("create table ub(" +
                            " user_id bigint, " +
                            " item_id bigint, " +
                            " category_id bigint, " +
                            " behavior string, " +
                            " ts bigint, " +
                            " et as to_timestamp_ltz(ts, 0), " +
                            " watermark for et as et - interval '3' second" +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/UserBehavior.csv', " +
                            " 'format' = 'csv'" +
                            ")");
        
        
        // 2. 过滤出pv, 统计每个商品每个窗口的点击量
        Table t1 = tEnv.sqlQuery("select " +
                                     " item_id, window_start, window_end," +
                                     " count(*) ct " +
                                     "from table( hop( table ub, descriptor(et), interval '30' minute, interval '2' hour) ) " +
                                     "where behavior='pv' " +
                                     "group by item_id, window_start, window_end");
        tEnv.createTemporaryView("t1", t1);
        // 3. 添加over窗口, 按照点击量进行排序
        // row_number rank dense_rank
        Table t2 = tEnv.sqlQuery("select" +
                                     " window_end w_end, " +
                                     " item_id, " +
                                     " ct item_count, " +
                                     " row_number() over(partition by window_start order by ct desc) rk " +
                                     "from t1");
        tEnv.createTemporaryView("t2", t2);
        
        // 4. 过滤出topN
        Table result = tEnv.sqlQuery("select * from t2 where rk<=3");
        
        // 5. 把结果写出到mysql中
        tEnv.executeSql("CREATE TABLE `hot_item` ( " +
                            "  `w_end` timestamp , " +
                            "  `item_id` bigint, " +
                            "  `item_count` bigint, " +
                            "  `rk` bigint, " +
                            "  PRIMARY KEY (`w_end`,`rk`) not enforced" +
                            ")with(" +
                            "  'connector' = 'jdbc'," +
                            "   'url' = 'jdbc:mysql://hadoop162:3306/flink_sql?useSSL=false'," +
                            "   'table-name' = 'hot_item', " +
                            "   'username' = 'root', " +
                            "   'password' = 'aaaaaa'" +
                            ")");
        
        result.executeInsert("hot_item");
        
        
    }
}
