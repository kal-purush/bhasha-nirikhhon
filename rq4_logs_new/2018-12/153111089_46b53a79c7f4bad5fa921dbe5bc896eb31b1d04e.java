package ua.nure.kn.dobriak.usermanagement.db;

import junit.framework.TestCase;

import ua.nure.kn.dobriak.usermanagement.User;

import java.time.LocalDate;

public class ManageUsersTest extends TestCase {
    private String FIRST_NAME = "Valeria";
    private String LAST_NAME = "Dobriak";
    private LocalDate DATE_OF_BIRTH = LocalDate.of(1998, 9, 30);

    public void testCreate() {
        User user = new User(FIRST_NAME, LAST_NAME, DATE_OF_BIRTH);
        Integer userId = ManageUser.create(user);
        assertNotNull(userId);
    }
}