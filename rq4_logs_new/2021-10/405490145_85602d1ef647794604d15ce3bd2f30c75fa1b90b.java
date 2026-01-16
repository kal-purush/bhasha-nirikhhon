package kz.onggar.collector.match;

import kz.onggar.collector.openapi.dto.Match;
import kz.onggar.collector.openapi.dto.MatchResult;
import kz.onggar.collector.openapi.dto.Player;
import kz.onggar.collector.openapi.dto.PlayerWithPlace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import javax.transaction.Transactional;
import java.util.List;

import static kz.onggar.collector.util.TestHelper.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = DEFINED_PORT)
@AutoConfigureMockMvc
class MatchTest {

    @Autowired
    private MockMvc mvc;

    private MatchResult matchResult;

    private Player player;

    @BeforeEach
    void init() {
        player = new Player();
        player.steamId("t1");
        player.relativeMmr(1000);
        player.simpleMmr(1000);
        player.competitiveMmr(1000);
    }


    @Test
    @Transactional
    void saveMatchResultTest() throws Exception {
        matchResult = new MatchResult();
        var mockPlayerWithPlace = new PlayerWithPlace();
        mockPlayerWithPlace.setPlace(4);
        mockPlayerWithPlace.setPlayerId(null);
        mockPlayerWithPlace.setSteamId("testSteamId");

        matchResult.setPlayersWithPlaces(List.of(mockPlayerWithPlace));

        var createdMatch = transformResponseToObject(
                makePostRequest(mvc, "/matches", matchResult, status().isCreated()),
                Match.class
        );

        var maybeCreatedPlayer = transformResponseToObject(
                makeGetRequest(mvc, "/players/steamId/" + mockPlayerWithPlace.getSteamId()), Player.class
        );

        assertAll("check that match have been created",
                () -> assertNotNull(createdMatch),
                () -> assertNotNull(createdMatch.getId()),
                () -> assertEquals(mockPlayerWithPlace.getSteamId(), maybeCreatedPlayer.getSteamId())
        );
    }
}