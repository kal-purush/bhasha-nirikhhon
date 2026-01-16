package com.joizhang.naiverpc.netty.serialize.java;

import com.joizhang.naiverpc.netty.serialize.SerializeSupport;
import com.joizhang.naiverpc.netty.serialize.User;
import com.joizhang.naiverpc.serialize.Serializer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class JavaSerializerTest {

    @Test
    public void testJavaSerializer() throws IOException, ClassNotFoundException {
        Serializer serializer = new JavaSerializer();

        int num = 1024;
        byte[] intBytes = SerializeSupport.serialize(serializer, num);
        assertNotNull(intBytes);
        int num1 = SerializeSupport.deserialize(serializer, intBytes, int.class);
        assertEquals(num, num1);

        User user = new User("Tom", 10);
        byte[] userBytes = SerializeSupport.serialize(serializer, user);
        assertNotNull(userBytes);
        User user1 = SerializeSupport.deserialize(serializer, userBytes, user.getClass());
        assertEquals(user, user1);
    }

}