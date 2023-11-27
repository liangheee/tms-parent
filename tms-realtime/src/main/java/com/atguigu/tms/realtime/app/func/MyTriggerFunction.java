package com.atguigu.tms.realtime.app.func;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO 总结一下TriggerResult
 *      TriggerResult是一个枚举类，一共有四个实例
 *      1、CONTINUE：表示对窗口不执行任何操作，即不触发窗口计算，也不删除元素
 *      2、FIRE：触发窗口计算，但是保留元素
 *      3、PURGE：不触发窗口计算，丢弃窗口，并且删除窗口元素
 *      4、FIRE_AND_PURGE：触发窗口计算，输出结果，然后将窗口中的数据和窗口进行清除
 *  Triiger中的定时器都是基于Flink Timer实现的，是一种感知并利用处理时间或事件时间变化的机制
 *  Flink的Timer会根据key+timestamp自动去重，也就是说如果我们Key有N个，并且注册的timestamp相同的话，那么实际只会注册N个Timer
 *
 * @author Hliang
 * @create 2023-10-01 23:18
 */
public class MyTriggerFunction<T> extends Trigger<T, TimeWindow> {

    /**
     * 只要有元素落入该窗口，就会调用该方法
     * @param element 收到的元素
     * @param timestamp 元素抵达的时间（注意时间语义）
     * @param window 元素所属的window窗口
     * @param ctx 上下文 通常用该对象注册 timer （ProcessTime | EventTime） 回调以及访问
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//        // 计算当前元素所处触发的时间范围的右边界点（闭）
//        long maxTimestamp = timestamp - timestamp % 10000L + 10000L - 1L;
//        if(maxTimestamp <= ctx.getCurrentWatermark()){
//            // 如果当前水位线 >= 当前元素所处的触发范围，那么就触发计算，但是不删除窗口内元素
//            // 当前水位线 = 当前元素所处的触发范围右边界点，并不能说明元素该范围内的元素就全部到齐
//            //  所以这里还会有 > 当前元素所处的触发范围右边界点这个说法（此时我们要通过onEventTime中的time == ctx.getCurrentWatermark判断是否fire）
//            //  乱序的我们就不会触发计算！！！
//            return TriggerResult.FIRE;
//        }else{
//            // 如果当前水位线并没有 大于或等于此时的maxTimestamp，注册定时器
//            ctx.registerEventTimeTimer(maxTimestamp);
//            return TriggerResult.CONTINUE;
//        }


        // 定义一个键控状态ValueState，来标识当前是否是该key下的第一个元素到来
        // 如果是，那么就创建对应的事件语义的定时器
        ValueStateDescriptor<Boolean> valueStateDescriptor = new ValueStateDescriptor<>("isFirstState", Types.BOOLEAN);
        ValueState<Boolean> isFirstState = ctx.getPartitionedState(valueStateDescriptor);
        Boolean isFirst = isFirstState.value();
        if(isFirst == null){
            // 当前key下的第一个元素到来时，isFirst肯定为null，此时创建定时器
//            ctx.registerEventTimeTimer(timestamp - timestamp % 10000L + 10000L - 1L);
            ctx.registerEventTimeTimer(timestamp - timestamp % 10000L + 10000L);
            // 更新状态中的值为true
            isFirstState.update(true);
        }else if(isFirst){
            // 说明前面已经有元素来过，
            isFirstState.update(false);
        }
        return TriggerResult.CONTINUE;
    }

    /**
     * proceessTime定时器触发时，就会调用该函数
     * @param time 定时器触发的时间
     * @param window 定时器触发的窗口对象
     * @param ctx 上下文 通常用该对象注册 timer （ProcessTime | EventTime） 回调以及访问
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    /**
     * eventTime定时器触发时，就会调用该函数
     * @param time 定时器触发的时间
     * @param window 定时器触发的窗口对象
     * @param ctx 上下文 通常用该对象注册 timer （ProcessTime | EventTime） 回调以及访问
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//        return time == ctx.getCurrentWatermark()? TriggerResult.FIRE : TriggerResult.CONTINUE;

        long end = window.getEnd(); // end不会减去1毫秒
        if(time < end){
            if(time + 10000L < end){
                ctx.registerEventTimeTimer(time + 10000L);
            }
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;

    }

    /**
     * 当窗口被删除后执行所需的任何操作。例如：可以清除定时器或者删除状态数据
     * @param window
     * @param ctx
     * @throws Exception
     */
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }
}
