package com.lib;

import com.lib.proto.ChefMap;
import com.lib.proto.ChefSpace;

import java.lang.reflect.Field;

public class Chef extends ChefSpace {

    ChefMap cache = new ChefMap();

    public void Chef() {

    }


    public <T> byte[] serialize(T input) {
        byte[] bytes = new byte[1024 * 1024 * 1];

        Class<?> k = input.getClass();
        Field[] fields = cache.getMapping(k);
        try {
            int offset = 0;
            for (Field field : fields) {
                Class<?> clazz = field.getType();
                Object valueAtField = field.get(input);
                offset = serializeOnto(valueAtField, clazz, bytes, offset);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return bytes;
    }

    public <T> T deSerialize(byte[] array, Class<T> clazz) {
        return null;
    }


}