package com.github.jmcampanini.hrm.emulator;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Value}.
 */
public class ValueTests {

    @Test
    public void of_constructor_works_with_strings() {
        String expectedValue = "a";
        Value a = Value.of(expectedValue);
        assertThat(a.value(), equalTo(expectedValue));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannot_be_empty_string() {
        Value.of("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannot_be_null_string() {
        Value.of(null);
    }
}