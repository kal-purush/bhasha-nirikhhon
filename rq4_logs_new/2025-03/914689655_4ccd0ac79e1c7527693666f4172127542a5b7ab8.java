package dataaccess;

import model.AuthData;
import org.junit.jupiter.api.*;
import server.handlers.services.CreateAuth;

import java.util.Objects;

public class AuthDAOTest {
    private static AuthDAO authMap;

    @BeforeAll
    public static void init() {
        try{
            authMap = new SQLAuthDAO();
        }catch(Exception e){

        }

    }

    @AfterEach
    public void cleanUp(){
        authMap.clearAllAuth();
    }

    @Test
    @DisplayName("Get Auth Success")
    public void getAuthSuccess(){
        AuthData testAuth = new AuthData(new CreateAuth(authMap).newToken(), "username");
        authMap.addAuth(testAuth);
        assert Objects.equals(authMap.getAuth(testAuth.authToken()).username(), testAuth.username());
    }

    @Test
    @DisplayName("Get Auth Fail")
    public void getAuthFail(){
        assert authMap.getAuth(new CreateAuth(authMap).newToken()) == null;
    }

    @Test
    @DisplayName("Add Auth Success")
    public void addAuthSuccess() {
        AuthData firstAuth = new AuthData(new CreateAuth(authMap).newToken(), "username");
        AuthData secondAuth = new AuthData(new CreateAuth(authMap).newToken(), "second Username");
        try {
            authMap.addAuth(firstAuth);
            authMap.addAuth(secondAuth);
            assert true;

        } catch (Exception e) {
            assert false;
        }
    }

    @Test
    @DisplayName("Add Auth Fail")
    public void addAuthFail(){
        AuthData firstAuth = new AuthData(null, null);
        try {
            authMap.addAuth(firstAuth);
        } catch (Exception e) {
            assert true;
        }

    }

    @Test
    @DisplayName("Remove Auth Success")
    public void removeAuthSuccess(){

        AuthData testAuth = new AuthData(new CreateAuth(authMap).newToken(), "username");
        authMap.addAuth(testAuth);
        Assertions.assertEquals(authMap.getAuth(testAuth.authToken()).username(), testAuth.username());
        authMap.removeAuth(testAuth.authToken());
        Assertions.assertEquals(authMap.getAuth(testAuth.authToken()), null);
    }

    @Test
    @DisplayName("Remove Auth Fail")
    public void removeAuthFail(){

        try{
            AuthData testAuth = new AuthData(new CreateAuth(authMap).newToken(), "username");
            authMap.addAuth(testAuth);
            authMap.removeAuth(null);
        }
        catch(Exception e){
            assert true;

        }

    }


    }


