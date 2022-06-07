package com.atguigu.flink.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/7 15:39
 */
public class AtguiguUtil {
    public static <T>List<T> toList(Iterable<T> it) {
        ArrayList<T> result = new ArrayList<>();
    
        for (T t : it) {
            result.add(t);
            
        }
        return result;
    }
}
