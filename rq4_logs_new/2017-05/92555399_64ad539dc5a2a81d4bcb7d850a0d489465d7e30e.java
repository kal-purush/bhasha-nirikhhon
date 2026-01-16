package com.example.examplecoverageas30;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MainActivityTest {

    @Test
    public void testAwesomeUtilityMethod() {
        assertThat(MainActivity.awesomeUtilityMethod(10, 2), is(0));
    }
}