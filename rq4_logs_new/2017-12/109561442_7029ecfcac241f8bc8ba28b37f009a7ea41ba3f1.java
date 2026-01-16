package com.gandazhi.sell.redisUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * @author gandazhi
 * @create 2017-12-11 下午9:10
 **/
@Component
@Slf4j
public class RedisLock {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 加锁
     *
     * @param key
     * @param value 当前时间+超时时间
     * @return
     */
    public boolean lock(String key, String value) {
        if (stringRedisTemplate.opsForValue().setIfAbsent(key, value)) { //redis命令 setnx key value  如果成功返回true,失败返回false
            return true;
        }

        String currentValue = stringRedisTemplate.opsForValue().get(key);
        //value中的值不为空且value中的值小于当前时间
        if (!StringUtils.isEmpty(currentValue) && Long.parseLong(currentValue) < System.currentTimeMillis()) {
            //获取上一个锁的时间
            String oldValue = stringRedisTemplate.opsForValue().getAndSet(key, value);
            if (!StringUtils.isEmpty(oldValue) && oldValue.equals(currentValue)) {
                return true;
            }
        }
        return false;
    }

    public void unlock(String key, String value) {
        try {
            String oldValue = stringRedisTemplate.opsForValue().get(key);
            if (!StringUtils.isEmpty(oldValue) && oldValue.equals(value)) {
                stringRedisTemplate.opsForValue().getOperations().delete(key);
            }
        }catch (Exception e){
            log.error("[redis解锁异常],{}",e);
        }
    }
}