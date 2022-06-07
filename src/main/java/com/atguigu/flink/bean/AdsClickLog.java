package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdsClickLog {
    private Long userId;
    private Long adsId;
    private String province;
    private String city;
    private Long timestamp;

}
