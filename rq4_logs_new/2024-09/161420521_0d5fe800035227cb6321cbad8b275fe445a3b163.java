package com.open.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * 接口防抖注解（仅作用于方法）
 * @author zc
 * @date 2024/9/20 16:50
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)// 运行时保留，这样才能在AOP中被检测到
public @interface AntiShake {
    /**
     * redis key 的前缀
     */
    String preKey() default "";

    /**
     * 默认防抖时间1秒
     */
    long value() default 1000L;


    /**
     * 时间单位,默认为毫秒
     */
    TimeUnit timeUnit() default TimeUnit.MILLISECONDS;
}