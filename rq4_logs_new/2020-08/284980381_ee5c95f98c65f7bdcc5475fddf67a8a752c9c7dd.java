package com.autohome.common.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisCore {
    //客户端对象
    private Jedis jedis;
    //构造函数
    public JedisCore(String host,int port,String pass){
        //连接池
        JedisPool pool=new JedisPool(host,port);
        //获取客户端对象
        jedis=pool.getResource();
        //设置密码
        jedis.auth(pass);
    }

    //新增
    public void set(String key,String val,int seconds){

        jedis.setex(key,seconds,val);
    }
    //删除
    public void del(String key){
        jedis.del(key);
    }
    //查询
    public String get(String key){
        return jedis.get(key);
    }

    //系统
    //校验key是否存在
    public boolean checkKey(String key){
        return jedis.exists(key);
    }

    //查询剩余时间
    public long ttl(String key){
        return jedis.ttl(key);
    }
    //查询指定前缀开头的key的数量
    public int keys(String key){
        return jedis.keys(key).size();
    }
    //设置key的有效期
    public void expire(String key,int seconds){
        jedis.expire(key,seconds);
    }
}