package com.atguigu.tms.publisher.utils;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @author Hliang
 * @create 2023-10-13 21:47
 */
public class DateFormatUtil {
    public static String now(){
        return DateFormatUtils.format(new Date(), "yyyy-MM-dd");
    }
}
