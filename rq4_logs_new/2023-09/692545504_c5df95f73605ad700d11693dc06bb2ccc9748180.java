package org.benbroadaway.unifi.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ApiCredentialsTest {

    @Test
    void testFromEnvironment() {
        var combined = "testUser:testPass".toCharArray();

        var creds = ApiCredentials.from(combined);

        assertEquals("testUser", creds.username());
        assertArrayEquals("testPass".toCharArray(), creds.password());
    }

    @Test
    void testNoSeparator() {
        var combined = "noSeparator".toCharArray();

        var ex = assertThrows(IllegalArgumentException.class,
                () -> ApiCredentials.from(combined));

        assertTrue(ex.getMessage().contains("Cannot find separator"));
    }
}