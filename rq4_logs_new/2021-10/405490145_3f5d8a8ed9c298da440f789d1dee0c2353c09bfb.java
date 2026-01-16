package kz.onggar.collector.player;

import kz.onggar.collector.openapi.dto.Player;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import javax.transaction.Transactional;

import static kz.onggar.collector.player.TestHelper.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = DEFINED_PORT)
@AutoConfigureMockMvc
class PlayerTest {

    @Autowired
    private MockMvc mvc;

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
    void createNewUserTest() throws Exception {
        var createdPlayer = transformResponseToObject(
                makePostRequest(mvc, "/players", player, status().isCreated()),
                Player.class
        );

        assertAll("check created player",
                () -> assertNotNull(createdPlayer.getId()),
                () -> assertEquals(createdPlayer.getCompetitiveMmr(), player.getCompetitiveMmr()),
                () -> assertEquals(createdPlayer.getRelativeMmr(), player.getRelativeMmr()),
                () -> assertEquals(createdPlayer.getSimpleMmr(), player.getSimpleMmr()),
                () -> assertEquals(createdPlayer.getSteamId(), player.getSteamId())
        );
    }

    @Test
    @Transactional
    void getAllPlayers() throws Exception {
        var createdPlayer = transformResponseToObject(
                makePostRequest(mvc, "/players", player, status().isCreated()),
                Player.class
        );

        var players = transformResponseToList(
                makeGetRequest(mvc, "/players"),
                Player.class
        );

        assertTrue(players.contains(createdPlayer));
    }

    @Test
    @Transactional
    void getPlayerById() throws Exception {
        var createdPlayer = transformResponseToObject(
                makePostRequest(mvc, "/players", player, status().isCreated()),
                Player.class
        );

        var player = transformResponseToObject(
                makeGetRequest(mvc, "/players/" + createdPlayer.getId()),
                Player.class
        );

        assertEquals(player, createdPlayer);
    }
}