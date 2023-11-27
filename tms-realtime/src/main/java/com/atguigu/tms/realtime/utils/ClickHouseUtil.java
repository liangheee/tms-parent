package com.atguigu.tms.realtime.utils;

import com.atguigu.tms.realtime.app.annotation.TransientSink;
import com.atguigu.tms.realtime.common.TmsConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Hliang
 * @create 2023-10-11 18:04
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getJdbcSink(String sql){
        SinkFunction<T> sinkFunction = JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        // 利用反射来给sql中的问号占位符赋值
                        Class clazz = obj.getClass();
                        // 获取所有属性
                        Field[] fields = clazz.getDeclaredFields();
                        int index = 1; // 通过ps给预编译sql赋值的位置索引（jdbc是从1开始计算）
                        for (Field field : fields) {
                            // 设置当前属性可以访问
                            field.setAccessible(true);
                            // 如果有这个注解，则跳过不处理
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if(annotation != null){
                                continue;
                            }
                            try {
                                // 获取当前属性的值
                                Object fieldVal = field.get(obj);
                                ps.setObject(index,fieldVal);
                                index++; // 索引++
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                JdbcExecutionOptions.builder()  // 攒批  （注意：它是针对于一个并行度而言来攒批的）
                        .withBatchSize(5000)  // 每5000条触发一次写入clickhouse
                        .withBatchIntervalMs(5000L) // 如超过5s还没有攒够5000条，也写入到clickhouse
                        .withMaxRetries(3) // 失败最大重试次数
                        .build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(TmsConfig.CLICKHOUSE_DRIVER)
                        .withUrl(TmsConfig.CLICKHOUSE_URL)
                        .build()
        );

        return sinkFunction;
    }
}
