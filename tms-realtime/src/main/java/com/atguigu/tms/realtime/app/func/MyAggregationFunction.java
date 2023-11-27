package com.atguigu.tms.realtime.app.func;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author Hliang
 * @create 2023-10-02 0:32
 */
public abstract class MyAggregationFunction<T> implements AggregateFunction<T,T,T> {
    /**
     * 初始化累加器
     * @return
     */
    @Override
    public T createAccumulator() {
        return null;
    }

    /**
     * 窗口触发计算结束后，调用该方法返回最终结果
     * @param accumulator
     * @return
     */
    @Override
    public T getResult(T accumulator) {
        return accumulator;
    }

    /**
     * 仅仅在会话窗口中会用到合并
     * @param a
     * @param b
     * @return
     */
    @Override
    public T merge(T a, T b) {
        return null;
    }
}
