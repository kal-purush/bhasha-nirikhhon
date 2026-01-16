package unit.tests;

import org.junit.jupiter.api.*;
import passoff.model.TestAuthResult;
import passoff.model.TestCreateRequest;
import passoff.model.TestUser;
import passoff.server.TestServerFacade;
import server.Server;

public class GameHandlerTest {
    @Test
    @DisplayName("Successful Game Made")
    public void createGame(){
     assert true;
    }

    @Test
    @DisplayName("Unsuccessful Game Creation- Unauthorized")
    public void createBadGame(){
        assert false;
    }

    @Test
    @DisplayName("Unsuccessful Game Creation- ")
    public void createBadGameTwo(){
        assert false;
    }

    @Test
    @DisplayName("Successful Join Game")
    public void joinGameGood(){
        assert true;
    }

    @Test
    @DisplayName("Unsuccessful Join Game")
    public void joinGameBad(){
        assert false;
    }

    @Test
    @DisplayName("Successfully List Games")
    public void listGameGood(){
        assert true;
    }

    @Test
    @DisplayName("Unsuccessfully List Games")
    public void listGameBad(){
        assert false;
    }

}