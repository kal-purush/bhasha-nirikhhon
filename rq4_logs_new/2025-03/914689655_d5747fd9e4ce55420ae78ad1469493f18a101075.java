package unit.tests;

import data.MemoryAuthDAO;
import data.MemoryUserDAO;
import model.AuthData;
import model.UserData;
import org.junit.jupiter.api.AfterEach;

import java.util.HashMap;
import java.util.UUID;

public class LoginTests {
    static MemoryUserDAO userMap = new MemoryUserDAO(new HashMap<String, UserData>());
    static MemoryAuthDAO authMap = new MemoryAuthDAO(new HashMap<UUID, AuthData>());
    @AfterEach
    public void cleanup(){
        userMap.clearAllUsers();
        authMap.clearAllAuth();
    }
}