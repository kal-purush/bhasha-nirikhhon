package tests;

import static org.junit.Assert.assertEquals;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import notdefault.Game;

public class GameTests {
    private int boardLength;
    private int boardWidth;
    private int playerAmount;
    private Game game;

    @Given("A board of length {int} and width {int} and {int} players")
    public void a_board_of_length_and_width_and_players(Integer int1, Integer int2, Integer int3) {
       boardLength = int1;
       boardWidth = int2;
       playerAmount = int3;
    }

    @When("Game starts")
    public void game_starts() {
        game = new Game();
        game.startGame(boardLength, boardWidth, playerAmount);
    }

    @Then("make a board of {int} x {int} with {int} robots on it")
    public void make_a_board_of_x_with_robots_on_it(Integer int1, Integer int2, Integer int3) {
       
	assertEquals(game.getBoard().getSize()[0],(int) int1);
	assertEquals(game.getBoard().getSize()[1],(int) int2);
	assertEquals(game.getBoard().getRobots().size(),(int) int3);
    }
}