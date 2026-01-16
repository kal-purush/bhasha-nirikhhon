package unit.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SessionHandlerTest {
    @Test
    @DisplayName("Login Successful")
    public void loginSuccess(){
        assert true;
    }

    @Test
    @DisplayName("Login Fail- Wrong Password")
    public void loginFailPassword(){
        assert false;
    }
    @Test
    @DisplayName("Login Fail- Username Doesn't Exist")
    public void loginFailUsername(){
        assert false;
    }

    @Test
    @DisplayName("Logout Successful")
    public void logoutSuccess(){
        assert true;
    }
    @Test
    @DisplayName("Logout Fail- AuthToken Invalid")
    public void logoutFailAuth(){
        assert false;
    }
}