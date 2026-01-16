package jaeseok.numble.mybox.common.redis;

import jaeseok.numble.mybox.util.annotation.IntegrationTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@IntegrationTest
public class RedisTemplateTest {

    @Autowired
    private RedisTemplate redisTemplate;

    @Test
    public void opsForValue() {
        // given
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        String key = "string_key";
        String value = "string_value";

        valueOperations.set(key, value);

        // when
        String result = valueOperations.get(key).toString();

        // then
        Assertions.assertEquals(value, result);
    }
}