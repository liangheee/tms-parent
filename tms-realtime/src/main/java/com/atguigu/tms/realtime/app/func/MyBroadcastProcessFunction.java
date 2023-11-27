package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.TmsConfigDimBean;
import com.atguigu.tms.realtime.common.TmsConfig;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * @author Hliang
 * @create 2023-09-25 18:36
 */
public class MyBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor;

    private HashMap<String,TmsConfigDimBean> tmsConfigMap = new HashMap<>();

    private String username = "root";

    private String password = "000000";

    public MyBroadcastProcessFunction(MapStateDescriptor mapStateDescriptor,String[] args){
        this.mapStateDescriptor = mapStateDescriptor;

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        username = parameterTool.get("mysql-username",username);
        password = parameterTool.get("mysql-password",password);
    }

    // 预加载配置表数据，解决主流和广播流数据的异步性（解决主流先来，广播流后来的问题）
    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        // 通过驱动管理器获取连接
        String url = "jdbc:mysql://hadoop102:3306/tms_config?useSSL=false&useUnicode=true" +
                "&user=" + username + "&password=" + password +
                "&charset=utf8&TimeZone=Asia/Shanghai&allowPublicKeyRetrieval=true";
        Connection connection = DriverManager.getConnection(url);

        // 预编译sql，获取ps对象
        PreparedStatement preparedStatement = connection.prepareStatement("select * from tms_config.tms_config_dim");

        // 执行查询，获取结果
        ResultSet resultSet = preparedStatement.executeQuery();

        // 对结果进行解析
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()){
            JSONObject jsonObj = new JSONObject();
            // 解析每一个列，放入json对象中
            for(int i = 1;i <= metaData.getColumnCount();i++){
                String columnName = metaData.getColumnName(i);
                String columnValue = resultSet.getString(i);
                jsonObj.put(columnName,columnValue);
            }
            // 将jsonObj转换为tmsConfigBean对象，封装到 tmsConfigMap中
            TmsConfigDimBean tmsConfigDimBean = jsonObj.toJavaObject(TmsConfigDimBean.class);
            tmsConfigMap.put(tmsConfigDimBean.getSourceTable(),tmsConfigDimBean);
        }
    }

    // 处理主流数据
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 从广播状态中获取维度表列表
        ReadOnlyBroadcastState<String, TmsConfigDimBean> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取主流数据中的表名
        String sourceTable = jsonObj.getString("table");
        // 判断是否是维度表
        TmsConfigDimBean tmsConfigDimBean = null;
        // 这里的判断顺序不能更改，改了导致一直都是map中
        if((tmsConfigDimBean = broadcastState.get(sourceTable)) != null || (tmsConfigDimBean = tmsConfigMap.get(sourceTable)) != null){

           // 是维度表，则对维度列进行过滤，去除不需要的列
            JSONObject afterJsonObj = jsonObj.getJSONObject("after");
            filterColumn(afterJsonObj,tmsConfigDimBean.getSinkColumns());

            // 数据脱敏
            switch (sourceTable) {
                // 员工表信息脱敏
                case "employee_info":
                    String empPassword = afterJsonObj.getString("password");
                    String empRealName = afterJsonObj.getString("real_name");
                    String idCard = afterJsonObj.getString("id_card");
                    String phone = afterJsonObj.getString("phone");

                    // 脱敏
                    empPassword = DigestUtils.md5Hex(empPassword);
                    empRealName = empRealName.charAt(0) +
                            empRealName.substring(1).replaceAll(".", "\\*");
                    //知道有这个操作  idCard是随机生成的，和标准的格式不一样 所以这里注释掉
                    // idCard = idCard.matches("(^[1-9]\\d{5}(18|19|([23]\\d))\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$)|(^[1-9]\\d{5}\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{2}$)")
                    //     ? DigestUtils.md5Hex(idCard) : null;
                    phone = phone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(phone) : null;

                    afterJsonObj.put("password", empPassword);
                    afterJsonObj.put("real_name", empRealName);
                    afterJsonObj.put("id_card", idCard);
                    afterJsonObj.put("phone", phone);
                    break;
                // 快递员信息脱敏
                case "express_courier":
                    String workingPhone = afterJsonObj.getString("working_phone");
                    workingPhone = workingPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(workingPhone) : null;
                    afterJsonObj.put("working_phone", workingPhone);
                    break;
                // 卡车司机信息脱敏
                case "truck_driver":
                    String licenseNo = afterJsonObj.getString("license_no");
                    licenseNo = DigestUtils.md5Hex(licenseNo);
                    afterJsonObj.put("license_no", licenseNo);
                    break;
                // 卡车信息脱敏
                case "truck_info":
                    String truckNo = afterJsonObj.getString("truck_no");
                    String deviceGpsId = afterJsonObj.getString("device_gps_id");
                    String engineNo = afterJsonObj.getString("engine_no");

                    truckNo = DigestUtils.md5Hex(truckNo);
                    deviceGpsId = DigestUtils.md5Hex(deviceGpsId);
                    engineNo = DigestUtils.md5Hex(engineNo);

                    afterJsonObj.put("truck_no", truckNo);
                    afterJsonObj.put("device_gps_id", deviceGpsId);
                    afterJsonObj.put("engine_no", engineNo);
                    break;
                // 卡车型号信息脱敏
                case "truck_model":
                    String modelNo = afterJsonObj.getString("model_no");
                    modelNo = DigestUtils.md5Hex(modelNo);
                    afterJsonObj.put("model_no", modelNo);
                    break;
                // 用户地址信息脱敏
                case "user_address":
                    String addressPhone = afterJsonObj.getString("phone");
                    addressPhone = addressPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(addressPhone) : null;
                    afterJsonObj.put("phone", addressPhone);
                    break;
                // 用户信息脱敏
                case "user_info":
                    String passwd = afterJsonObj.getString("passwd");
                    String realName = afterJsonObj.getString("real_name");
                    String phoneNum = afterJsonObj.getString("phone_num");
                    String email = afterJsonObj.getString("email");

                    // 脱敏
                    passwd = DigestUtils.md5Hex(passwd);
                    if(StringUtils.isNotEmpty(realName)){
                        realName = DigestUtils.md5Hex(realName);
                        afterJsonObj.put("real_name", realName);
                    }
                    phoneNum = phoneNum.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(phoneNum) : null;
                    email = email.matches("^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$")
                            ? DigestUtils.md5Hex(email) : null;

                    afterJsonObj.put("birthday", DateFormatUtil.toDate(afterJsonObj.getInteger("birthday") * 24 * 60 * 60 * 1000L));
                    afterJsonObj.put("passwd", passwd);
                    afterJsonObj.put("phone_num", phoneNum);
                    afterJsonObj.put("email", email);
                    break;
            }


            // 向下游传递数据，仅仅需要传递部分数据
            afterJsonObj.put("sink_table",tmsConfigDimBean.getSinkTable());
            afterJsonObj.put("sink_pk",tmsConfigDimBean.getSinkPk());
            if(StringUtils.isEmpty(tmsConfigDimBean.getSinkFamily())){
                afterJsonObj.put("sink_family", TmsConfig.HBASE_DEFAULT_COLUMN_FAMILY);
            }

            // 加入一些辅助下游删除Redis中维度缓存的辅助信息
            String op = jsonObj.getString("op");
            JSONObject beforeJsonObj = jsonObj.getJSONObject("before");
            String foreignKeys = tmsConfigDimBean.getForeignKeys();
            // 如果当前为更新维度操作，并且外键字段不为空
            if("u".equals(op)){
                ArrayList<Tuple2<String, String>> nameAndValueList = new ArrayList<>();
                // 添加主键信息
                Tuple2<String,String> rowKeyNameAndValue = Tuple2.of("id",beforeJsonObj.getString("id"));
                nameAndValueList.add(rowKeyNameAndValue);
                // 添加外键信息
                if(StringUtils.isNotEmpty(foreignKeys)){
                    // 按照逗号分隔外键行，获取多个外键
                    String[] keys = foreignKeys.split(",");
                    // 封装为2元组的形式传递给下游
                    for (String key : keys) {
                        String value = beforeJsonObj.getString(key);
                        Tuple2<String, String> foreignKeyNameAndValue = Tuple2.of(key, value);
                        nameAndValueList.add(foreignKeyNameAndValue);
                    }
                }
                // 添加到jsonObj中，传递到下游
                afterJsonObj.put("nameAndValueList",nameAndValueList);
            }
            out.collect(afterJsonObj);
        }

    }

    /**
     * 过滤业务数据库中维度表的冗余字段
     * @param afterJsonObj 业务数据库中维度表每行数据的json对象
     * @param sinkColumns HBase中对应维度表所需要的维度属性
     */
    private void filterColumn(JSONObject afterJsonObj, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> filterArr = Arrays.asList(columns);
        Set<Map.Entry<String, Object>> entrySet = afterJsonObj.entrySet();
        entrySet.removeIf(entry -> ! filterArr.contains(entry.getKey()));
    }

    // 处理广播流
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, TmsConfigDimBean> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // op有r、c、u、d
        // 如果当前op=d，我们要将广播状态中的数据删除
        // 如果当前op=c、r、u，我们需要将其添加到广播状态中
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        JSONObject afterObj = jsonObject.getJSONObject("after");
        String op = jsonObject.getString("op");
        if("d".equals(op)){
            broadcastState.remove(afterObj.getString("source_table"));
            tmsConfigMap.remove(afterObj.getString("source_table"));
        }else {
            // 如果op=c、r、u则加入到广播状态中
            TmsConfigDimBean tmsConfigDimBean = afterObj.toJavaObject(TmsConfigDimBean.class);
            String sourceTable = tmsConfigDimBean.getSourceTable();
            broadcastState.put(sourceTable,tmsConfigDimBean);
        }
    }
}
