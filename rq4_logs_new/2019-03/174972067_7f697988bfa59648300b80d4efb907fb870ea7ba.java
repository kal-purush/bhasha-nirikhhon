package nl.sean.dea;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.*;

class LoginResourceTest {
    private LoginResource sut;

    @BeforeEach
    void setUp() {
        sut = new LoginResource();
    }

    @Test
    void loginSuccess() {
        UserDTO user = new UserDTO("Sean", "test");
        Response actualResult = sut.logUserIn(user);

        assertEquals(Response.Status.OK.getStatusCode(), actualResult.getStatus());
    }


}