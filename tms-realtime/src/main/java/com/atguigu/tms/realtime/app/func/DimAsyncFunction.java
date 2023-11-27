package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.common.TmsConfig;
import com.atguigu.tms.realtime.utils.DimUtil;
import com.atguigu.tms.realtime.utils.ThreadPoolUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * TODO 发送异步请求进行维度关联
 *  模板方法设计模式：
 *      在父类中定义完成某一个功能的核心算法的骨架（步骤），有些步骤具体的实现延迟到子类中去完成
 *      在不改变父类核心算法骨架的前提下，每一个子类都可以由自己不同的实现
 *
 * @author Hliang
 * @create 2023-10-04 23:03
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> {

    // 维度关联的维度表名称
    private String tableName;

    // 关联字段是否为主键
    private boolean isPrimaryKey;

    private ExecutorService executorService;

    public DimAsyncFunction(String tableName,boolean isPrimaryKey) {
        this.tableName = tableName;
        this.isPrimaryKey = isPrimaryKey;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 连接池对象很重，初始化open方法中加载一次即可
        executorService = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                // 1、获取关联维度的主键或者外键
                Tuple2<String,String> keyNameAndValue = getCondition(input);

                // 2、通过DimUtil获取关联维度信息
                JSONObject dimJsonObj = DimUtil.getDimInfo(TmsConfig.HBASE_NAMESPACE,tableName,isPrimaryKey,keyNameAndValue);

                // 3、关联
                if(dimJsonObj != null){
                    join(input,dimJsonObj);
                }

                // 4、将结果传递到下游
                resultFuture.complete(Collections.singleton(input));
            }

        });



    }

    public abstract void join(T input, JSONObject dimJsonObj);

    public abstract Tuple2<String, String> getCondition(T input);
}
