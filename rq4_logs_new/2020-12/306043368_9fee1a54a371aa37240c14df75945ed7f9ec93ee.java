package org.example.common.util;

import java.util.Random;
import java.util.UUID;

/**
 * @author walle
 * @version V1.0
 * @since 2020-12-14 14:40
 */
public class RandomUtil {

    private static Random random = new Random();

    /**
     * uuid
     *
     * @return uuid string
     */
    public static String uuid() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString().replace("-", "").toUpperCase();
    }

    /**
     * 生成随机整数
     *
     * @param bound 边界值
     * @return int
     */
    public static int randomInt(int bound) {
        return random.nextInt(bound);
    }
}