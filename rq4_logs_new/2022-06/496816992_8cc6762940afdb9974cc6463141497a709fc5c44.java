package com.revature.test;
import com.revature.database.DatabaseHandler;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertAll;

public class JUnitTests {

    @Test
    @DisplayName("DatabaseHandler getUser from username and password")
    public void test1(){
        assertAll(() -> assertEquals("js123", DatabaseHandler.getDbHandler().getUser("js123", "admin").getUsername()),
                () -> assertEquals("tl123", DatabaseHandler.getDbHandler().getUser("tl123", "admin").getUsername()),
                () -> assertEquals("JohnDoe", DatabaseHandler.getDbHandler().getUser("JohnDoe", "123").getUsername()));
    }
    @Test
    @DisplayName("DatabaseHandler getUserByEmail from username and email")
    public void test2(){
        assertAll(() -> assertEquals("admin", DatabaseHandler.getDbHandler().getUserByEmail("js123", "gwfds123@yahoo.com").getPassword()),
                () -> assertEquals("admin", DatabaseHandler.getDbHandler().getUserByEmail("tl123", "tl@gmail.com").getPassword()),
                () -> assertEquals("123", DatabaseHandler.getDbHandler().getUserByEmail("JohnDoe", "hacofa2021@sceath.com").getPassword()));
    }
}

