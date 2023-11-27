package com.atguigu.tms.realtime.common;

/**
 * @author Hliang
 * @create 2023-09-25 16:54
 */
public class TmsConfig {
    public static String HBASE_NAMESPACE = "tms_realtime";
    public static String HBASE_DEFAULT_COLUMN_FAMILY = "info";
    public static String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";
    public static String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/tms_realtime";
}
