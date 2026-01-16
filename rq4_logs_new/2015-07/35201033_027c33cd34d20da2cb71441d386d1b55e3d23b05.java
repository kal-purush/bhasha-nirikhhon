
package com.welick.rs.redis.jedis.demo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class DButil {
	//�� org.apache.commons.pool.impl.GenericObjectPool ���Կ���һЩ������Ĭ��ֵ
	//����IP
	private static final String  HOST="localhost";
	//�˿�
	private static final Integer PORT=6379;
	//��֤����
	private static final String AUTH_PAS="xstao";
	//�����Ŀ
	private static final Integer MAX_ACTIVE =20;
	//ָ��pool�� ����ж��ٸ�״̬Ϊidle 
	private static final Integer MAX_IDLE=100;
	//��ȴ�ʱ�� ����
	private static final Integer MAX_WAIT=5000;
	//��ʱ
	private static final Integer TIME_OUT=5000;
	
	private static JedisPool poolInstance=null;
	/***
	 * ��ȡjedis ��ʵ������
	 * @date 2015��7��14�� ����11:38:15
	 * @author xs Tao 
	 * @return
	 */
	public static JedisPool getJedisPoolInstance(){
		if(poolInstance==null){
			//����redis���ݿ� �Ĳ�������
			JedisPoolConfig config=new JedisPoolConfig();
			config.setMaxActive(MAX_ACTIVE);
			config.setMaxWait(MAX_WAIT);
			config.setMaxIdle(MAX_IDLE);
			//������ ӵ�ж�����캯��
			//�����Ĭ�ϵĶ˿ں�  Ҳ����ʹ��Ĭ�϶˿ںŵĹ��캯������������Ҳ�ǿ���Ĭ�ϵġ�
			poolInstance=new JedisPool(config, HOST, PORT, TIME_OUT, AUTH_PAS);			
		}
		return poolInstance;
	}
	public static Jedis getJedis(){
		if(poolInstance==null){
			DButil.getJedisPoolInstance();
		}
		//�ӳ������� ��ȡһ��jedis��Դ
		Jedis jedis= poolInstance.getResource();
		return jedis;
	}
	/**
	 * ����jedis����
	 * @date 2015��7��14�� ����11:43:03
	 * @author xs Tao 
	 * @param jedis
	 */
	public static void destory(Jedis jedis){
		if(poolInstance!=null && jedis!=null){
			poolInstance.returnResource(jedis);
		}
	}
}