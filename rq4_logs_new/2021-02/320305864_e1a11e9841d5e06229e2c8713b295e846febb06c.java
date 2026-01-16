package daohuei.leetcodelizard;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ReverseIntegerTest {
    @Test
    public void testOnReverseInteger() {
        assertEquals("Reverse Integer", 321, ReverseInteger.reverse(123), 0);
        assertEquals("Reverse Integer", -321, ReverseInteger.reverse(-123), 0);
        assertEquals("Reverse Integer", 21, ReverseInteger.reverse(120), 0);
        assertEquals("Reverse Integer", 0, ReverseInteger.reverse(0), 0);
    }
}