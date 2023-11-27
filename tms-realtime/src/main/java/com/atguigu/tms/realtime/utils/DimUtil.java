package com.atguigu.tms.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.common.TmsConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author Hliang
 * @create 2023-10-04 13:11
 */
public class DimUtil {
    /**
     * 先从Redis中获取维度，如果不存在则从硬盘中获取，再存储到Redis
     * @param namespace 名称空间
     * @param tableName 表名
     * @param isRowKey 是否为rowKey
     * @param nameAndValue rowKey的名称和值
     * @return
     */
    public static JSONObject getDimInfo(String namespace, String tableName, boolean isRowKey, Tuple2<String,String> nameAndValue){
        String name = nameAndValue.f0;
        String value = nameAndValue.f1;
        // 拼接Redis中的key
        String key = "dim:" + tableName + ":" + name + "_" + value;
        Jedis jedis = null;
        JSONObject dimJsonObj = null;
        try {
            jedis = JedisUtil.getJedis();
            String jsonStr = jedis.get(key);
            if(StringUtils.isNotEmpty(jsonStr)){
                // 如果从redis中获取缓存成功
                dimJsonObj = JSON.parseObject(jsonStr);
            }else{
                // 如果从redis中获取缓存失败，则从HBase中获取
                if(isRowKey){
                    // 如果是主键，则调用HBase从主键中获取维度的方法
                    dimJsonObj = HBaseUtil.getRowByPrimaryKey(namespace,tableName,value);
                }else {
                    // 如果是外键，则调用HBase从外键中获取维度的方法
                    List<JSONObject> dimJsonObjList = HBaseUtil.getRowByForeignKey(namespace, tableName, TmsConfig.HBASE_DEFAULT_COLUMN_FAMILY, Tuple2.of(name, value));
                    // 其实dimJsonObjList中只会有一条记录
                    if(dimJsonObjList != null && dimJsonObjList.size() > 0){
                        dimJsonObj = dimJsonObjList.get(0);
                    }
                }

                // 将dimJsonObj缓存到Redis中
                if(dimJsonObj != null){
                    // redis缓存1天
                    jedis.setex(key,24 * 60 * 60,JSON.toJSONString(dimJsonObj));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(jedis != null) JedisUtil.closeJedis(jedis);
        }

        return dimJsonObj;
    }

    public static void delDimCache(String tableName,Tuple2<String,String> nameAndValue){
        String name = nameAndValue.f0;
        String value = nameAndValue.f1;
        String key = "dim:" + tableName + ":" + name + "_" + value;
        Jedis jedis = null;
        try {
            jedis = JedisUtil.getJedis();
            jedis.del(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(jedis != null) JedisUtil.closeJedis(jedis);
        }
    }



}
