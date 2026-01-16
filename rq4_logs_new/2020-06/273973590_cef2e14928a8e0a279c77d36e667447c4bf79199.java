package com.revature.onlinestoreapp.models;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class AdminTest {

    Admin adminTest = new Admin();

    @Test
    public void checkEmail(){

        adminTest.setEmail("adminmaster@gmail.com");
        assertEquals("adminmaster", adminTest.getEmail());

    }

    @Test
    public void checkFirstName(){

        adminTest.setFirstName("Dwight");
        assertEquals("Dwight", adminTest.getFirstName());

    }

    @Test
    public void checkLastName(){

        adminTest.setLastName("Shrute");
        assertEquals("Shrute", adminTest.getLastName());

    }

    @Test
    public void createCustomerTest(){

        Admin newAdmin =
                new Admin("Andy","Bernard", "narddog@dunder.com", "cornell");

        assertEquals(newAdmin.getPassword(), "cornell");

    }
}