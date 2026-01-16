package net.zeeraa.novacore.commons.utils;

import java.lang.reflect.Field;

/**
 * Class with utils for java.lang.reflect
 */
public class ReflectUtils {

    public static Object getPrivateField(String fieldName, Class<?> clazz, Object object) {
        Field field;
        Object o = null;
        try {
            field = clazz.getDeclaredField(fieldName);

            field.setAccessible(true);

            o = field.get(object);
        } catch(NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return o;
    }

}