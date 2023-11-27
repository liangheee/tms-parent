package com.atguigu.tms.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author Hliang
 * @create 2023-09-26 11:55
 */
public class DateFormatUtil {
    // 定义 年-月-日 日期格式化对象
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // 定义 年-月-日 时:分:秒 日期格式化对象
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将格式化日期字符串转换为时间戳
     *
     * @param dtStr 格式化日期字符串
     * @param isFull 是否包含 时:分:秒
     * @return 毫秒时间戳
     */
    public static Long toTs(String dtStr, boolean isFull) {

        LocalDateTime localDateTime = null;
        if (!isFull) {
            dtStr = dtStr + " 00:00:00";
        }
        localDateTime = LocalDateTime.parse(dtStr, dtfFull);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        // .toInstant(ZoneOffset.of("Z")).toEpochMilli();
    }

    /**
     * 将 年-月-日 格式的日期字符串转换为时间戳
     *
     * @param dtStr 年-月-日 格式的日期字符串
     * @return 时间戳
     */
    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
    }

    /**
     * 将毫秒时间戳转换为 年-月-日 格式的日期字符串
     *
     * @param ts 毫秒时间戳
     * @return 格式化日期字符串
     */
    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        // = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.of("Z"));
        return dtf.format(localDateTime);
    }

    /**
     * 将毫秒时间戳转换为 年月日 格式的日期字符串
     *
     * @param ts 毫秒时间戳
     * @return 格式化日期字符串
     */
    public static String toPartitionDate(Long ts) {
        return toDate(ts).replaceAll("-", "");
    }

    /**
     * 将毫秒时间戳转换为 年-月-日 时:分:秒 格式的日期字符串
     *
     * @param ts 毫秒时间戳
     * @return 格式化日期字符串
     */
    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        // = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.of("Z"));
        return dtfFull.format(localDateTime);
    }
}
