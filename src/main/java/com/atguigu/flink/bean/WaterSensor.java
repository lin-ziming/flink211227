package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    //pojo  : 字段 + setter+getter+构造器   用来封装数据
    
    private String id;
    private Long ts;
    private Integer vc;
}
