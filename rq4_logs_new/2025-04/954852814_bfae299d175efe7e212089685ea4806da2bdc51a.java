package com.example.auth_service.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Аннотация для ограничения частоты запросов к API.
 * Применяется к методам контроллеров для защиты от брутфорса и DDoS атак.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {
    /**
     * Максимальное количество запросов за временное окно.
     * @return количество запросов
     */
    int value() default 10;

    /**
     * Временное окно в секундах.
     * @return время в секундах
     */
    int timeWindow() default 60;

    /**
     * Пользовательский ключ для Redis.
     * Если не указан, будет сгенерирован автоматически.
     * @return ключ для Redis
     */
    String key() default "";
} 