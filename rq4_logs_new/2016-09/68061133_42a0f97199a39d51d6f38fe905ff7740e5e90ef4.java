package com.minitwit.model;

import org.junit.Test;
import static org.junit.Assert.*;

public class UserTest {
    @Test
    public void testInputValidation() {
        User user = new User();
        user.setUsername("john.smith");
        user.setEmail("john.smith@example.com");
        user.setPassword("not-empty");
        user.setPassword2("not-empty");

        assertNull(user.validate());
    }
}