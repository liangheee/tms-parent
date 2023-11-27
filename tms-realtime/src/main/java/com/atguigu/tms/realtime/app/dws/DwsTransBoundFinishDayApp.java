package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransTransFinishBean;
import com.atguigu.tms.realtime.beans.DwsTransBoundFinishDayBean;
import com.atguigu.tms.realtime.utils.ClickHouseUtil;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-10-12 21:18
 */
public class DwsTransBoundFinishDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1 创建流处理环节，设置检查点
        // 1.2 设置并行度
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        env.setParallelism(4);

        // TODO 2、读取Kafka主题数据
        // 2.1 声明消费kafka主题
        String topic = "tms_dwd_trans_bound_finish_detail";
        // 2.2 声明消费者id
        String groupId = "dws_trans_bound_finish_day_group";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("dws_trans_bound_finish_day_kafka_source");

        // TODO 3、转换读取数据jsonStr为专用结构，将 ts + 8小时
        SingleOutputStreamOperator<DwsTransBoundFinishDayBean> mappedDS = kafkaStrDS.map(new MapFunction<String, DwsTransBoundFinishDayBean>() {
            @Override
            public DwsTransBoundFinishDayBean map(String jsonStr) throws Exception {
                DwdTransTransFinishBean dwdTransTransFinishBean = JSONObject.parseObject(jsonStr, DwdTransTransFinishBean.class);
                DwsTransBoundFinishDayBean dwsTransBoundFinishDayBean = DwsTransBoundFinishDayBean.builder()
                        .boundFinishOrderCountBase(1L)
                        .ts(dwdTransTransFinishBean.getTs() + 8 * 60 * 60 * 1000)
                        .build();
                return dwsTransBoundFinishDayBean;
            }
        });

        // TODO 4、分配窗口和抽取ts
        SingleOutputStreamOperator<DwsTransBoundFinishDayBean> withWatermarkDS = mappedDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTransBoundFinishDayBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransBoundFinishDayBean>() {
                    @Override
                    public long extractTimestamp(DwsTransBoundFinishDayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // TODO 5、开窗（全窗口）
        AllWindowedStream<DwsTransBoundFinishDayBean, TimeWindow> withWindowDS = withWatermarkDS.windowAll(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 6、关联自定义触发器
        AllWindowedStream<DwsTransBoundFinishDayBean, TimeWindow> withTriggerDS = withWindowDS.trigger(new MyTriggerFunction<>());

        // TODO 7、聚合
        SingleOutputStreamOperator<DwsTransBoundFinishDayBean> aggregateDS = withTriggerDS.aggregate(
                new MyAggregationFunction<DwsTransBoundFinishDayBean>() {
                    @Override
                    public DwsTransBoundFinishDayBean add(DwsTransBoundFinishDayBean dwsTransBoundFinishDayBean, DwsTransBoundFinishDayBean accumulator) {
                        if(accumulator == null){
                            return dwsTransBoundFinishDayBean;
                        }
                        accumulator.setBoundFinishOrderCountBase(dwsTransBoundFinishDayBean.getBoundFinishOrderCountBase() + accumulator.getBoundFinishOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessAllWindowFunction<DwsTransBoundFinishDayBean, DwsTransBoundFinishDayBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTransBoundFinishDayBean> elements, Collector<DwsTransBoundFinishDayBean> out) throws Exception {
                        long start = context.window().getStart();
                        for (DwsTransBoundFinishDayBean element : elements) {
                            element.setCurDate(DateFormatUtil.toYmdHms(start - 8 * 60 * 60 * 1000));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        aggregateDS.print(">>>");

        // TODO 8、写入clickhouse
        String sql = "insert into table dws_trans_bound_finish_day_base values (?,?,?)";
        aggregateDS.addSink(ClickHouseUtil.getJdbcSink(sql)).uid("dws_trans_bound_finish_day_sink");

        // TODO 9、启动flink
        env.execute();
    }
}
