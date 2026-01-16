package ru.otus.utils;

import java.lang.reflect.Field;

public final class ReflectionUtils {

    private ReflectionUtils() {}

    public static void injectValue(Object target, Field field, Object value) {
        try {
            field.setAccessible(true);
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isNullField(Object target, Field field) {
        try {
            field.setAccessible(true);
            return field.get(target) == null;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}