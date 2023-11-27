package com.atguigu.tms.realtime.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Hliang
 * @create 2023-10-04 15:47
 * 使用双重检查锁机制实现懒汉式
 */
public class ThreadPoolUtil {

    // volatile增强其可见性
    private static volatile ThreadPoolExecutor threadPoolExecutor;

    public static ThreadPoolExecutor getThreadPoolExecutor(){
        if(threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class) {
                if(threadPoolExecutor == null){
                    System.out.println("~~~创建线程池~~~");
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return threadPoolExecutor;
    }
}
