package com.demo.test;

/**
 * Created by SAI on 7/29/2016.
 */
import org.testng.annotations.*;
public class FirstTest {
    @BeforeClass

    public void setUp() {
        System.out.println("TEST");
        System.out.println(System.getProperty("config.paramfile"));
        System.out.println("Setp up");
        // code that will be invoked when this test is instantiated
    }

    @Test(groups = { "fast" })
    public void aFastTest() {
        System.out.println("Fast test");
    }

    @Test(groups = { "slow" })
    public void aSlowTest() {
        System.out.println("Slow test");
    }
}