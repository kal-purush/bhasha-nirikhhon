package Tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Objects;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import rebuild.ActionCard;
import rebuild.Board;
import rebuild.CardType;
import rebuild.Game;
import rebuild.Player;
import rebuild.Robot;
import rebuild.Tile;
import rebuild.TyleType;
import rebuild.Obstacle;

public class obstacleTest {
    private int length;
    private int width;
    private int playerAmount;
    private int difficulty;
    private Game game;
    private Robot robot;


    @Given("len {int} wid {int} playerAmount {int} and diff {int}")
    public void len_wid_player_amount_and_diff(Integer int1, Integer int2, Integer int3, Integer int4) {
	length = int1; width = int2; playerAmount = int3; difficulty = int4;
    }

    @When("game started")
    public void game_starts() {
	game = new Game(length,width,playerAmount,difficulty);
    }

    @When("turn started")
    public void turn_started() {
        game.startTurn();
    }

    @When("players chose their cards")
    public void players_chose_their_cards() {
	for(Player player : game.getPlayers()) {
	    ArrayList<ActionCard> hand = new ArrayList<ActionCard>();

	    hand.add(new ActionCard(CardType.Move1));hand.add(new ActionCard(CardType.Move1));
	    hand.add(new ActionCard(CardType.Move1));hand.add(new ActionCard(CardType.Move1));
	    
	    player.setHand(hand);
	}
    }

    @When("cards are being executed")
    public void cards_are_being_executed() {
	game.execMoves();
    }
    
    @When("a barrel tile")
    public void a_barrel_tile() {
	Board brd = game.getBoard();
	Tile tile = new Tile(2);
	Obstacle obstacle = new Obstacle("Barrel");
	
	tile.setObstacle(obstacle); tile.setType(TyleType.Pit);
	brd.setTile(1,0,tile);
        game.setBoard(brd);
        
        robot = game.getRobots().get(0);
    }
    
    @When("a pit tile")
    public void a_pit_tile() {
	Board brd = game.getBoard();
	Tile tile = new Tile(2);
	Obstacle obstacle = new Obstacle("Pit");
	
	tile.setObstacle(obstacle); tile.setType(TyleType.Pit);
	brd.setTile(1,0,tile);
        game.setBoard(brd);
        
        robot = game.getRobots().get(0);
    }
    
    @When("a health tile")
    public void a_health_tile() {
	Board brd = game.getBoard();
	Tile tile = new Tile(2);
	Obstacle obstacle = new Obstacle("Health");
	
	tile.setObstacle(obstacle); tile.setType(TyleType.Pit);
	brd.setTile(1,0,tile); brd.setTile(2,0,tile);
	brd.setTile(3,0,tile); brd.setTile(4,0,tile);
        game.setBoard(brd);
    }

    @Then("obstacles are generated on tiles")
    public void obstacles_are_generated_on_tiles() {
	for(Tile[] column : game.getBoard().getTiles()) {
	    for(Tile row : column) {
		assertFalse(Objects.isNull(row.getObstacle()));
	    }
	}
    }
    
    @Then("robot should not move into the barrel")
    public void robot_should_not_move_into_the_barrel() {
        assertArrayEquals(game.getRobots().get(0).getCoordinate(),new int[] {0,0});
    }
    
    @Then("robot should die")
    public void robot_should_die() {
	assertFalse(robot.equals(game.getRobots().get(0)));
    }
    
    @Then("robots life should go up")
    public void robots_life_should_go_up() {
        assertTrue(3 < game.getRobots().get(0).getLives());
    }
}