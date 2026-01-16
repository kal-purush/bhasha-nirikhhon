package com.allstate.compozed.springplayground.home;

import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.Assert.*;

public class HomeModelTest {
    @Test
    public void getMessageMustBePublic () throws NoSuchMethodException {
        final Method getMessage = HomeModel.class.getDeclaredMethod("getMessage");
        assertTrue(Modifier.isPublic(getMessage.getModifiers()));
    }

    @Test
    public void getMessageReturnsConstructorMessage () {
        final HomeModel homeModel1 = new HomeModel("message");
        final HomeModel homeModel2 = new HomeModel("other message");

        assertEquals("message", homeModel1.getMessage());
    }
}