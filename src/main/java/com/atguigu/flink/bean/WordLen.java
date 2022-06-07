package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/3/12 11:48
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordLen {
    private String word;
    private Integer len;
}
