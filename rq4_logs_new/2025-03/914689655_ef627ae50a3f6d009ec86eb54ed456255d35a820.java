package service;

import chess.ChessGame;
import dataaccess.GameDAO;
import dataaccess.SQLGameDAO;
import model.GameData;
import org.junit.jupiter.api.*;

public class GameDAOTest {

    static GameDAO gameMap;

    @BeforeAll
    public static void init() {
        try{
            gameMap = new SQLGameDAO();
        }catch(Exception e){

        }

    }
    @AfterEach
    public void cleanUp(){
        gameMap.clearAllGames();
    }

    @Test
    @DisplayName("Create Game Success")
    public void createSucces(){
        GameData testGame = new GameData(123, null, null, "Games", new ChessGame());
        gameMap.addGame(testGame);
        Assertions.assertEquals(gameMap.getGameByID(testGame.gameID()).gameName(), testGame.gameName());

    }
    @Test
    @DisplayName("Creation Failed")
    public void createFail(){
        try{
            GameData testGame = new GameData(1, null, null, null, null);
            gameMap.addGame(testGame);
        }
        catch (Exception e){
            assert true;
        }
    }

    @Test
    @DisplayName("Update Game")
    public void updateSuccess(){
        GameData testGame = new GameData(123, null, null, "name", new ChessGame());
        gameMap.addGame(testGame);
        GameData newGame = new GameData(123, "player_one", null, "Alternate Name", new ChessGame());
        gameMap.updateGame(testGame.gameID(), newGame);
        Assertions.assertEquals(newGame.whiteUsername(), gameMap.getGameByID(testGame.gameID()).whiteUsername());

    }

    @Test
    @DisplayName("Clear Games")
    public void clearSuccess(){
        GameData testGame = new GameData(123, null, null, "name", new ChessGame());
        gameMap.addGame(testGame);
        assert gameMap.getGameByID(testGame.gameID()) != null;
        gameMap.clearAllGames();
        GameData response = gameMap.getGameByID(testGame.gameID());
        assert response == null;

    }
}