package com.openpromt.coffeee.swf2023.openpromtserver.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class DeserializationUtil {
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object object = null;
        try {
            object = ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ois.close();
        return object;
    }
}