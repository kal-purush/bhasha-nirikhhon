package com.joizhang.naiverpc.netty.utils;

import java.util.Date;

public final class ReflectUtils {

    private ReflectUtils() {
    }

    public static boolean isPrimitives(Class<?> cls) {
        while (cls.isArray()) {
            cls = cls.getComponentType();
        }
        return isPrimitive(cls);
    }

    public static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() ||
                cls == String.class ||
                cls == Boolean.class ||
                cls == Character.class ||
                Number.class.isAssignableFrom(cls) ||
                Date.class.isAssignableFrom(cls);
    }

}