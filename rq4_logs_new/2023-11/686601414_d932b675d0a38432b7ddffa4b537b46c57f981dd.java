package com.indevsolutions.workshop.bet.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;

import com.indevsolutions.workshop.bet.dto.BetDTO;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BetControllerTest {

	@Autowired
	private BetController betController;

	@LocalServerPort
	private int port;

	@Autowired
	private TestRestTemplate restTemplate;

	@Test
	void index() {
		assertThat(betController).isNotNull();
	}

	@Test
	void testBets() {
		var betResponse = restTemplate.exchange("http://localhost:" + port + "/workshop/bets", HttpMethod.GET, null,
				new ParameterizedTypeReference<List<BetDTO>>() {
				});
		assertNotNull(betResponse);

		var bets = betResponse.getBody();
		assertNotNull(bets);
		assertEquals(2, bets.size());
	}

	@Test
	void testBetsByLeagueId() {
		var betResponse = restTemplate.exchange("http://localhost:" + port + "/workshop/bets?leagueId={leagueId}",
				HttpMethod.GET, null, new ParameterizedTypeReference<List<BetDTO>>() {
				}, 1);
		assertNotNull(betResponse);

		var bets = betResponse.getBody();
		assertNotNull(bets);
		assertEquals(1, bets.size());
		assertEquals(1, bets.get(0).getLeagueId());
	}
	
	@Test
	void testBetsById() {
		var betResponse = restTemplate.exchange("http://localhost:" + port + "/workshop/bets?id={id}",
				HttpMethod.GET, null, new ParameterizedTypeReference<List<BetDTO>>() {
				}, 2);
		assertNotNull(betResponse);

		var bets = betResponse.getBody();
		assertNotNull(bets);
		assertEquals(1, bets.size());
		assertEquals(2, bets.get(0).getId());
	}
	
	@Test
	void testBetsByStatus() {
		var betResponse = restTemplate.exchange("http://localhost:" + port + "/workshop/bets?status={status}",
				HttpMethod.GET, null, new ParameterizedTypeReference<List<BetDTO>>() {
				}, 2);
		assertNotNull(betResponse);

		var bets = betResponse.getBody();
		assertNotNull(bets);
		assertEquals(1, bets.size());
		assertEquals(2, bets.get(0).getStatus());
	}
	
	@Test
	void testBetById() {
		var betResponse = restTemplate.exchange("http://localhost:" + port + "/workshop/bets/{id}", HttpMethod.GET, null ,BetDTO.class,2);
		var bet = betResponse.getBody();
		assertNotNull(bet);
		assertEquals(2, bet.getId());
	}
}