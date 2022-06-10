package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.HotItem;
import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/8 15:20
 */
public class Flink03_High_TopN {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // pojo  javabean
    
        env
            .readTextFile("input/UserBehavior.csv")
            .map(new MapFunction<String, UserBehavior>() {
                @Override
                public UserBehavior map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new UserBehavior(
                        Long.valueOf(data[0]),
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2]),
                        data[3],
                        Long.parseLong(data[4]) * 1000
                    );
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ub, ts) -> ub.getTimestamp())
            )
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .keyBy(UserBehavior::getItemId)
            .window(SlidingEventTimeWindows.of(Time.hours(2), Time.hours(1)))
            .aggregate(
                new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                
                    @Override
                    public Long add(UserBehavior value, Long acc) {
                        return acc + 1;
                    }
                
                    @Override
                    public Long getResult(Long acc) {
                        return acc;
                    }
                
                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                },
                new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long key,
                                        Context ctx,
                                        Iterable<Long> elements,
                                        Collector<HotItem> out) throws Exception {
                        Long count = elements.iterator().next();
                    
                        out.collect(new HotItem(key, ctx.window().getEnd(), count));
                    
                    
                    }
                }
            )
            .keyBy(HotItem::getWEnd)
            .process(new KeyedProcessFunction<Long, HotItem, String>() {
    
                private ListState<HotItem> hotItemState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    hotItemState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                }
    
                @Override
                public void processElement(HotItem value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 当某个key的第一个数据过来, 注册定时器
                    // 每条数据都存入到状态中, 当定时器触发的时候,进行排序取topN
                    Iterator<HotItem> it = hotItemState.get().iterator();
                    
                    if (!it.hasNext()) {  // 迭代器中如果没有值, 则表示是第一个元素进来
                        // 注册定时器
                        ctx.timerService().registerEventTimeTimer(value.getWEnd() + 2000);
                    }
                    
                    // 把数据存入到状态中
                    hotItemState.add(value);
                    
                }
    
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
    
                    List<HotItem> list = AtguiguUtil.toList(hotItemState.get());
                    
                    /*list.sort(new Comparator<HotItem>() {
                        @Override
                        public int compare(HotItem o1, HotItem o2) {
                            return o2.getCount().compareTo(o1.getCount())
                        }
                    });*/
                    list.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));
                    
                    String msg = "\n------------------------\n";
                    for (int i = 0; i < Math.min(3, list.size()); i++) {
                        msg += list.get(i) + "\n";
                    }
    
                    out.collect(msg);
                    
    
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
