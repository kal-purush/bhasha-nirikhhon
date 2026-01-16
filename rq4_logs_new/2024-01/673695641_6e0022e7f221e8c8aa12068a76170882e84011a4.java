package com.ran.storage.platform.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * OperateLog
 * 记录对接口的操作
 * @author rwei
 * @since 2024/1/8 14:53
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OperateLog {
    String module() default "";

    String type() default "";

    String desc() default "";
}