package com.atguigu.tms.realtime.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;

/**
 * @author Hliang
 * @create 2023-09-24 14:10
 */
public class CreateEnvUtil {
    /**
     * 创建环境对象
     * 开启、配置checkpoint
     * 配置重启策略
     * 设置访问hdfs的用户
     * @param args 动态配置参数
     * @return 返回 StreamExecutionEnvironment env
     */
    public static StreamExecutionEnvironment createStreamEnv(String[] args){
        // 创建环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启、配置checkpoint
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
        // 配置checkpoint的存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/realtime-tms01/ck");
        // 配置checkpoint关闭后是否保存
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 配置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        // 配置两个checkpoint发生的间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);

        // 配置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1),Time.seconds(3)));

        // 通过ParameterTool解析args参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hadoopUserName = parameterTool.get("hadoop-user-name", "liangheee");

        // 设置访问hdfs用户
        System.setProperty("HADOOP_USER_NAME",hadoopUserName);

        return env;
    }

    /**
     * 创建flink-cdc中的mysqlSource
     * @param args 动态配置参数
     * @param option 取值dwd | realtime-dim （选择读取dwd层数据还是dim层数据）
     * @param serverId flink-cdc模拟mysql主从复制中的从机，serverId的范围为5400 ~ 6400
     * @return 返回 MySqlSource<String>
     */
    public static MySqlSource<String> createMysqlSource(String[] args,String option,String serverId){

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 获取主机名
        String hostName = parameterTool.get("mysql-hostname", "hadoop102");
        // 获取端口号
        int port = Integer.parseInt(parameterTool.get("mysql-port", "3306"));
        // 获取用户名
        String username = parameterTool.get("mysql-username", "root");
        // 获取密码
        String passwd = parameterTool.get("mysql-passwd", "000000");
        // 获取serverId
       serverId = parameterTool.get("server-id", serverId);

       if(serverId == null || "".equals(serverId)){
           throw new RuntimeException("server-id不能为空！");
       }

        // 读取MySQL数据
        MySqlSourceBuilder<String> mySqlSourceBuilder = MySqlSource.<String>builder()
                .hostname(hostName)
                .port(port)
                .username(username)
                .serverId(serverId)
                .password(passwd);

        // 创建反序列化器，将其中的Decimal数据类型的反序列化默认实现改为NUMERRIC
        // 因为默认Decimal反序列化为Base64，我们改为反序列化为NUMERIC
        HashMap config = new HashMap();
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        // 将前面Map集合中的配置信息传递给JSON解析Schema，该Schema将用于MysqlSource的初始化
        JsonDebeziumDeserializationSchema jsonDebeziumDeserializationSchema = new JsonDebeziumDeserializationSchema(false,config);

        // 根据option区分加载dwd数据还是dim数据
        switch (option){
            case "dwd":
                // 读取事实数据
                String[] dwdTables = new String[]{"tms.order_info",
                        "tms.order_cargo",
                        "tms.transport_task",
                        "tms.order_org_bound"};
                return mySqlSourceBuilder.databaseList("tms")
                        .tableList(dwdTables)
                        // dwd层数据一直从最新开始读
                        .startupOptions(StartupOptions.latest())
                        .deserializer(jsonDebeziumDeserializationSchema)
                        .build();
            case "realtime-dim":
                // 读取维度数据
                String[] realtimeDimTables = new String[]{"tms.user_info",
                        "tms.user_address",
                        "tms.base_complex",
                        "tms.base_dic",
                        "tms.base_region_info",
                        "tms.base_organ",
                        "tms.express_courier",
                        "tms.express_courier_complex",
                        "tms.employee_info",
                        "tms.line_base_shift",
                        "tms.line_base_info",
                        "tms.truck_driver",
                        "tms.truck_info",
                        "tms.truck_model",
                        "tms.truck_team"};

                return mySqlSourceBuilder.databaseList("tms")
                        .tableList(realtimeDimTables)
                        // dim层数据，首次同步要同步所有mysql历史数据，之后仅同步当天dim数据
                        .startupOptions(StartupOptions.initial())
                        .deserializer(jsonDebeziumDeserializationSchema)
                        .build();
            case "tms_config_dim":
                String[] dimTmsConfig = new String[]{"tms_config.tms_config_dim"};
                return mySqlSourceBuilder.databaseList("tms_config")
                        .tableList(dimTmsConfig)
                        // dim层数据，首次同步要同步所有mysql历史数据，之后仅同步当天dim数据
                        .startupOptions(StartupOptions.initial())
                        .deserializer(jsonDebeziumDeserializationSchema)
                        .build();

            default:
                throw new RuntimeException("加载原始数据类型失败！，仅支持对dwd层或者dim层数据进行加载！");
        }

    }
}
