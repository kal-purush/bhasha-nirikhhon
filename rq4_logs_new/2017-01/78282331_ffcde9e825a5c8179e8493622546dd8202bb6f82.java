package com.games.controller;

import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.games.dto.ApplicationConstant;
import com.games.dto.BaseResponse;
import com.games.dto.GameResponse;
import com.games.entity.Game;
import com.games.enums.ChipColor;
import com.games.enums.GameMode;
import com.games.enums.GameState;
import com.games.enums.Player;
import com.games.service.IGameInitService;
import com.google.gson.Gson;

@ContextConfiguration(locations = { "classpath:TestCreateGameController-context.xml" })
@RunWith(SpringJUnit4ClassRunner.class)
public class TestCreateGameController {

	private Gson gson = new Gson();

	@Autowired
	private CreateGameController createGameController;

	@Autowired
	private IGameInitService gameInitService;

	@Test
	public void testChooseColor() {
		ResponseEntity<Map<ChipColor, Byte>> resps = createGameController.chooseColor();
		Assert.assertNotNull(resps);
		Assert.assertTrue(resps.getBody().get(ChipColor.BLUE) == 2);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCreateGame() {
		Mockito.reset(gameInitService);
		Mockito.when(gameInitService.createGame(Mockito.any(ChipColor.class), Mockito.any(ChipColor.class),
				Mockito.any(GameMode.class))).thenReturn(populateGame());
		ResponseEntity<GameResponse> resp = (ResponseEntity<GameResponse>) createGameController
				.createGame(ChipColor.BLUE, new MockHttpSession());
		Assert.assertNotNull(resp);
		Assert.assertEquals(resp.getBody().getTurn(), Player.PLAYER_1);
		Assert.assertEquals(resp.getBody().getState(), GameState.IN_PROGRESS);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCreateGameWithSession() {
		Mockito.reset(gameInitService);
		String id = UUID.randomUUID().toString();
		GameResponse gameResponse = new GameResponse();
		gameResponse.setGameId(id);
		MockHttpSession session = new MockHttpSession();
		session.setAttribute(ApplicationConstant.GAME_ID, gson.toJson(gameResponse));
		Mockito.when(gameInitService.getById(Mockito.anyString())).thenReturn(populateGame());
		Mockito.when(gameInitService.updateGame(Mockito.any(Game.class))).thenReturn(populateGame());
		ResponseEntity<GameResponse> resp = (ResponseEntity<GameResponse>) createGameController
				.createGame(ChipColor.RED, session);
		Assert.assertNotNull(resp);
		Assert.assertEquals(resp.getBody().getTurn(), Player.PLAYER_1);
		Assert.assertEquals(resp.getBody().getState(), GameState.IN_PROGRESS);
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testDeleteGame() {
		Mockito.reset(gameInitService);
		String id = UUID.randomUUID().toString();
		GameResponse gameResponse = new GameResponse();
		gameResponse.setGameId(id);
		MockHttpSession session = new MockHttpSession();
		session.setAttribute(ApplicationConstant.GAME_ID, gson.toJson(gameResponse));
		Mockito.when(gameInitService.delete(Mockito.anyString())).thenReturn(populateGame());
		ResponseEntity<BaseResponse> resp = (ResponseEntity<BaseResponse>) createGameController
				.deleteGame(session);
		Assert.assertNotNull(resp);
		Assert.assertTrue(resp.getStatusCode() == HttpStatus.OK);
	}

	private Game populateGame() {
		Game game = new Game();
		game.setId(UUID.randomUUID().toString());
		game.setGameMode(GameMode.SINGLE_PLAYER);
		game.setPlayerOneColor(ChipColor.BLUE);
		game.setOpponentColor(ChipColor.RED);
		return game;

	}

}