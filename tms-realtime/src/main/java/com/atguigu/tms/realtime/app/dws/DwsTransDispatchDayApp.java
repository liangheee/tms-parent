package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransDispatchDetailBean;
import com.atguigu.tms.realtime.beans.DwsTransDispatchDayBean;
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
 * @create 2023-10-12 21:32
 */
public class DwsTransDispatchDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1 准备流处理环境 设置检查点
        // 1.2 设置并行度
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        env.setParallelism(4);

        // TODO 2、从kafka主题读取数据
        // 2.1 声明消费的主题
        String topic = "tms_dwd_trans_dispatch_detail";
        // 2.2 声明消费者id
        String groupId = "tms_dwd_trans_dispatch_detail_group";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("tms_dwd_trans_dispatch_detail_kafka_source");

        // TODO 3、转换读取数据jsonStr 为 专用结构，将ts + 8小时
        SingleOutputStreamOperator<DwsTransDispatchDayBean> mappedDS = kafkaStrDS.map(new MapFunction<String, DwsTransDispatchDayBean>() {
            @Override
            public DwsTransDispatchDayBean map(String jsonStr) throws Exception {
                DwdTransDispatchDetailBean dwdTransDispatchDetailBean = JSONObject.parseObject(jsonStr, DwdTransDispatchDetailBean.class);
                DwsTransDispatchDayBean dwsTransDispatchDayBean = DwsTransDispatchDayBean.builder()
                        .dispatchOrderCountBase(1L)
                        .ts(dwdTransDispatchDetailBean.getTs() + 8 * 60 * 60 * 1000)
                        .build();
                return dwsTransDispatchDayBean;
            }
        });

        // TODO 4、分配水位线，抽取ts
        SingleOutputStreamOperator<DwsTransDispatchDayBean> withWatermarkDS = mappedDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTransDispatchDayBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransDispatchDayBean>() {
                    @Override
                    public long extractTimestamp(DwsTransDispatchDayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // TODO 5、开窗（全窗口）
        AllWindowedStream<DwsTransDispatchDayBean, TimeWindow> withWindowDS = withWatermarkDS.windowAll(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 6、关联自定义触发器
        AllWindowedStream<DwsTransDispatchDayBean, TimeWindow> withTriggerDS = withWindowDS.trigger(new MyTriggerFunction<>());

        // TODO 7、聚合
        SingleOutputStreamOperator<DwsTransDispatchDayBean> aggregateDS = withTriggerDS.aggregate(
                new MyAggregationFunction<DwsTransDispatchDayBean>() {
                    @Override
                    public DwsTransDispatchDayBean add(DwsTransDispatchDayBean dwsTransDispatchDayBean, DwsTransDispatchDayBean accumulator) {
                        if(accumulator == null){
                            return dwsTransDispatchDayBean;
                        }
                        accumulator.setDispatchOrderCountBase(dwsTransDispatchDayBean.getDispatchOrderCountBase() + accumulator.getDispatchOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessAllWindowFunction<DwsTransDispatchDayBean, DwsTransDispatchDayBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTransDispatchDayBean> elements, Collector<DwsTransDispatchDayBean> out) throws Exception {
                        long start = context.window().getStart();
                        for (DwsTransDispatchDayBean element : elements) {
                            element.setCurDate(DateFormatUtil.toYmdHms(start - 8 * 60 * 60 * 1000));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        aggregateDS.print(">>>");

        // TODO 8、写入clickhosue
        String sql = "insert into table dws_trans_dispatch_day_base values (?,?,?)";
        aggregateDS.addSink(ClickHouseUtil.getJdbcSink(sql)).uid("dws_trans_dispatch_day_base_sink");

        // TODO 9、执行flink
        env.execute();
    }
}
