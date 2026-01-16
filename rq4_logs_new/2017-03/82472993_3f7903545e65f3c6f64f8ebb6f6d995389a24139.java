package com.kvteam.backend.services;

import com.kvteam.backend.dataformats.UserData;
import com.kvteam.backend.exceptions.AccessDeniedException;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by maxim on 12.03.17.
 */
public class AccountServiceTest {
    private AccountService accountService;

    @Before
    public void setup(){
        accountService = new AccountService();
    }

    private boolean addUser(@NotNull String username){
        return accountService.add(
                new UserData(
                        username,
                        "passwd",
                        "my@email.ru",
                        null,
                        null
                )
        );
    }

    @Test
    public void addCorrectly(){
        assertTrue(addUser("user1"));
    }

    @Test
    public void addConflict(){
        assertTrue(addUser("user2"));
        assertFalse(addUser("user2"));
    }

    @Test
    public void getCorrectly(){
        assertTrue(addUser("user3"));

        final UserData data = accountService.get("user3");
        assertNotNull(data);
        assertEquals("user3", data.getUsername());
        assertNull(data.getPassword());
        assertEquals("my@email.ru", data.getEmail());
    }

    @Test
    public void getNotFound(){
        final UserData data = accountService.get("not_exist");
        assertNull(data);
    }

    @Test
    public void loginLogoutCorrectly(){
        assertTrue(addUser("user4"));
        final UUID sessionID = accountService.login("user4", "passwd");
        assertNotNull(sessionID);
        assertTrue(accountService.isLoggedIn("user4", sessionID));
        accountService.tryLogout("user4", sessionID);
        assertFalse(accountService.isLoggedIn("user4", sessionID));
    }

    @Test
    public void loginAccessDenied(){
        assertTrue(addUser("user5"));
        assertNull(accountService.login("IamNotExist", "passwd"));
        assertNull(accountService.login("user5", "incorrect passwd"));
    }

    @Test
    public void editEmail(){
        assertTrue(addUser("user6"));
        final UUID sessionID = accountService.login("user6", "passwd");
        assertNotNull(sessionID);
        try {
            accountService.editAccount(
                    "user6",
                    sessionID,
                    "new@mail.ru",
                    null);
        }catch(AccessDeniedException e){
            assertNotNull(e);
        }
        final UserData data = accountService.get("user6");
        assertNotNull(data);
        assertEquals("user6", data.getUsername());
        assertNull(data.getPassword());
        assertEquals("new@mail.ru", data.getEmail());
    }


    @Test
    public void editPassword(){
        assertTrue(addUser("user7"));
        final UUID sessionID = accountService.login("user7", "passwd");
        assertNotNull(sessionID);
        try {
            accountService.editAccount(
                    "user7",
                    sessionID,
                    null,
                    "newpasswd");
        }catch(AccessDeniedException e){
            assertNotNull(e);
        }
        accountService.tryLogout("user7", sessionID);
        final UUID sessionID2 = accountService.login("user7", "passwd");
        assertNull(sessionID2);
        final UUID sessionID3 = accountService.login("user7", "random");
        assertNull(sessionID3);
        final UUID sessionID4 = accountService.login("user7", "newpasswd");
        assertNotNull(sessionID4);
    }

    @Test
    public void editAccessDenied(){
        assertTrue(addUser("user8"));
        assertTrue(addUser("user9"));
        final UUID sessionID = accountService.login("user9", "passwd");
        assertNotNull(sessionID);
        boolean raised = false;
        try {
            accountService.editAccount(
                    "user8",
                    sessionID,
                    null,
                    "newpasswd");
        }catch(AccessDeniedException e){
            raised = true;
        }
        assertTrue(raised);
    }
}