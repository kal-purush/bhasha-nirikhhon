package com.banzhuan.util;

import redis.clients.jedis.Jedis;

public class JedisTest
{
	public static void main(String[] args)
	{
		Jedis jj = new  Jedis("localhost");
        jj.set("key1", "I am value 1");
        String ss = jj.get("helloword");
        jj.del("key1");
        jj.del("helloword");
        System.out.println(ss);
	}
}