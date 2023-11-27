package com.atguigu.tms.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Hliang
 * @create 2023-10-03 15:30
 */
public class JedisUtil {
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(1000);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000L);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
    }

    public static Jedis getJedis(){
        System.out.println("~~~获取Jedis客户端~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    public static void closeJedis(Jedis jedis){
        jedis.close();
        System.out.println("~~~关闭Jedis客户端~~~");
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        if(jedis != null) jedis.close();
        System.out.println("关闭Jedis");
    }
}
