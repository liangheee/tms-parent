package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.common.TmsConfig;
import com.atguigu.tms.realtime.utils.DimUtil;
import com.atguigu.tms.realtime.utils.HBaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * @author Hliang
 * @create 2023-09-26 12:39
 */
public class DimSinkFunction implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取写入HBase的表和RowKey
        String sinkTable = jsonObj.getString("sink_table");
        jsonObj.remove("sink_table");
        String sinkPk = jsonObj.getString("sink_pk");
        jsonObj.remove("sink_pk");
        String sinkFamily = jsonObj.getString("sink_family");

        ArrayList<Tuple2<String, String>> nameAndValueList = (ArrayList<Tuple2<String, String>>) jsonObj.get("nameAndValueList");
        jsonObj.remove("nameAndValueList");

        Set<Map.Entry<String, Object>> entrySet = jsonObj.entrySet();
        Put put = new Put(Bytes.toBytes(jsonObj.getString(sinkPk)));
        // 遍历jsonObj中每一个元素
        for (Map.Entry<String, Object> entry : entrySet) {
            if(!sinkPk.equals(entry.getKey())){
                put.addColumn(Bytes.toBytes(sinkFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }
        }

        // 删除redis中的旧维度数据（可能同一条dim被主键和外键同时缓存，所以可能一次需要删除多条）
        if(nameAndValueList != null && nameAndValueList.size() > 0){
            for (Tuple2<String, String> nameAndValue : nameAndValueList) {
                DimUtil.delDimCache(sinkTable,nameAndValue);
            }
        }

        // 更新或者添加数据
        HBaseUtil.putRow(TmsConfig.HBASE_NAMESPACE,sinkTable,put);
    }
}
