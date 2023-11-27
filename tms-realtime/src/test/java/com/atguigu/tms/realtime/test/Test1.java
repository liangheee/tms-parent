package com.atguigu.tms.realtime.test;

import com.atguigu.tms.realtime.utils.DateFormatUtil;

import java.util.Date;
import java.util.Random;

/**
 * @author Hliang
 * @create 2023-10-01 12:34
 */
public class Test1 {
    public static void main(String[] args) {
        long millis = System.currentTimeMillis();
        System.out.println(millis);
        System.out.println(DateFormatUtil.toYmdHms(millis));
    }
}
