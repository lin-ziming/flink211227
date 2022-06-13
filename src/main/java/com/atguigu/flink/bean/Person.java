package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/3/11 16:21
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private String a;
    private String b;
    private String c;
    private Long ts;
    private Integer score;
}
