package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/3/8 11:05
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotItem {
    private Long itemId;  // 商品id
    private Long wEnd;  // 窗口结束时间
    private Long count; // 商品在对应的窗口内的点击量
}
