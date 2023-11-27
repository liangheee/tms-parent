package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdBoundSortBean;
import com.atguigu.tms.realtime.beans.DwsBoundOrgSortDayBean;
import com.atguigu.tms.realtime.utils.ClickHouseUtil;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author Hliang
 * @create 2023-10-01 22:00
 */
public class DwsBoundOrgSortDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境的准备
        // 1.1 创建流处理环境，设置检查点
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        // 1.2 设置全局并行度
        env.setParallelism(4);

        // TODO 2、读取dwd层tms_dwd_bound_sort主题的数据
        String topic = "tms_dwd_bound_sort";
        String groupId = "dwd_bound_sort_consumer";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> dwdKafkaSource = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

//        dwdKafkaSource.print("!!!");

        // TODO 3、将读取的数据jsonStr转换为专用数据结构DwsBoundOrgSortDayBean，并设置初始count=1（同时解决时区不一致问题 ts + 8小时）
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> dwsBoundOrgSortDayDS = dwdKafkaSource.map(new MapFunction<String, DwsBoundOrgSortDayBean>() {
            @Override
            public DwsBoundOrgSortDayBean map(String jsonStr) throws Exception {
                // TODO 为了解决@Builder注解带来的DwdBoundSortBean默认构造器移除的问题，我们在DwdBoundSortBean中手动添加上@NoConstructor和@AllConstructor注解
                DwdBoundSortBean dwdBoundSortBean = JSON.parseObject(jsonStr, DwdBoundSortBean.class);
                return DwsBoundOrgSortDayBean.builder()
                        .orgId(dwdBoundSortBean.getOrgId())
                        .sortCountBase(1L)
                        // 同时解决时区不一致问题 ts + 8小时
                        .ts(dwdBoundSortBean.getTs() + 8 * 60 * 60 * 1000L)
                        .build();
            }
        });

//        dwsBoundOrgSortDayDS.print("@@@");

        // TODO 4、设置Watermark和抽取事件时间语义字段
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withWatermarkDS = dwsBoundOrgSortDayDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsBoundOrgSortDayBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsBoundOrgSortDayBean>() {
                            @Override
                            public long extractTimestamp(DwsBoundOrgSortDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 5、进行keyBy分组操作
        KeyedStream<DwsBoundOrgSortDayBean, String> keyedDS = withWatermarkDS.keyBy(DwsBoundOrgSortDayBean::getOrgId);

        // TODO 6、开窗，窗口大小为1天的滚动窗口
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 7、绑定自定义触发器 每隔10秒钟触发
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> triggerDS = windowDS.trigger(new MyTriggerFunction<DwsBoundOrgSortDayBean>());

        // TODO 8、聚合计算
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> aggregateDS = triggerDS.aggregate(
                new MyAggregationFunction<DwsBoundOrgSortDayBean>() {
                    @Override
                    public DwsBoundOrgSortDayBean add(DwsBoundOrgSortDayBean value, DwsBoundOrgSortDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setSortCountBase(accumulator.getSortCountBase() + 1);
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsBoundOrgSortDayBean, DwsBoundOrgSortDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsBoundOrgSortDayBean> elements, Collector<DwsBoundOrgSortDayBean> out) throws Exception {
                        for (DwsBoundOrgSortDayBean element : elements) {
                            long start = context.window().getStart();
                            // 减去8小时，从零时区当日8点到次日8点  ===》 零时区的0点到24点
                            element.setCurDate(DateFormatUtil.toDate(start - 8 * 60 * 60 * 1000L));
                            // 因为我们聚合了很多状态，选哪一个状态的ts都不合适，所以这里我们选择使用当前的系统时间作为ts
                            element.setTs(System.currentTimeMillis());

                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate");

//        aggregateDS.print(">>>");

        // TODO 9、关联其它维度信息<机构、城市、省份>
        // 关联机构名称和父级机构ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withOrgNameAndJoinOrgIdDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ", true) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean, JSONObject dimJsonObj) {
                        String orgName = dimJsonObj.getString("org_name");
                        String orgParentId = dimJsonObj.getString("org_parent_id");
                        dwsBoundOrgSortDayBean.setOrgName(orgName);

                        // 如果父org_id为null，那么其就是转运中心，没有父亲节点，否则存在父亲节点
                        String joinOrgId = orgParentId == null ? dwsBoundOrgSortDayBean.getOrgId() : orgParentId;
                        dwsBoundOrgSortDayBean.setJoinOrgId(joinOrgId);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean) {
                        String orgId = dwsBoundOrgSortDayBean.getOrgId();
                        return Tuple2.of("id",orgId);
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 关联城市ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityIdDS = AsyncDataStream.unorderedWait(
                withOrgNameAndJoinOrgIdDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ", true) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean, JSONObject dimJsonObj) {
                        String regionId = dimJsonObj.getString("region_id");
                        dwsBoundOrgSortDayBean.setCityId(regionId);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean) {
                        String joinOrgId = dwsBoundOrgSortDayBean.getJoinOrgId();
                        return Tuple2.of("id", joinOrgId);
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 关联城市和省份Id
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityNameAndProvinceIdDS = AsyncDataStream.unorderedWait(
                withCityIdDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info", true) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean, JSONObject dimJsonObj) {
                        String parentId = dimJsonObj.getString("parent_id");
                        String dictCode = dimJsonObj.getString("dict_code");
                        String name = dimJsonObj.getString("name");

                        // 设置城市的名称
                        dwsBoundOrgSortDayBean.setCityName(name);

                        // 如果当前是城市，那么设置其省份id
                        if("City".equals(dictCode)){
                            dwsBoundOrgSortDayBean.setProvinceId(parentId);
                        }
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean) {
                        String cityId = dwsBoundOrgSortDayBean.getCityId();
                        return Tuple2.of("id",cityId);
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withProvinceNameDS = AsyncDataStream.unorderedWait(
                withCityNameAndProvinceIdDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info", true) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean, JSONObject dimJsonObj) {
                        String name = dimJsonObj.getString("name");
                        dwsBoundOrgSortDayBean.setProvinceName(name);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean) {
                        String provinceId = dwsBoundOrgSortDayBean.getProvinceId();
                        return Tuple2.of("id",provinceId);
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        withProvinceNameDS.print(">>>");


        // TODO 10、写入ClickHouse
        String sql = "insert into table dws_bound_org_sort_day_base values (?,?,?,?,?,?,?,?,?)";
        SinkFunction<DwsBoundOrgSortDayBean> jdbcSink = ClickHouseUtil.<DwsBoundOrgSortDayBean>getJdbcSink(sql);

        withProvinceNameDS.addSink(jdbcSink).uid("dws_bound_org_sort_day_base_sink_to_clickhosue");


        // TODO 11、执行Flink任务
        env.execute();
    }
}
