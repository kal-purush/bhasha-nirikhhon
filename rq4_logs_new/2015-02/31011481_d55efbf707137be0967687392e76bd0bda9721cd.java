package com.example.myappgradle.app;

import junit.framework.TestCase;

public class helloWorldGradleTest extends TestCase {


    private helloWorldGradle hello;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        hello = new helloWorldGradle();
    }


    public void testGetIntegerFromNative() throws Exception {


        final int a=hello.getIntegerFromNative(5);

        final int expected = 10;
        final int reality = a;
        assertEquals(expected, reality);


    }
}