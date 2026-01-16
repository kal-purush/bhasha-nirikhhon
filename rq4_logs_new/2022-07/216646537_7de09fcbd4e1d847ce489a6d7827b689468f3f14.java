package com.mjamsek.rest.test.tests;

import com.mjamsek.rest.exceptions.BadRequestException;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExceptionsTest {
    
    @Test
    public void badRequest() {
        Object[] params = new Object[]{1, "test", false};
        var e1 = new BadRequestException("error.code");
        var e2 = new BadRequestException("error.code", params);
        assertEquals(400, e1.getStatus());
        assertArrayEquals(params, e2.getResponse().getParams());
    }
    
}