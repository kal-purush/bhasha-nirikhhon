package nl.sean.dea.resource;

import nl.sean.dea.dto.ErrorDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PlaylistResourceTest {
    private PlaylistResource sut;
    @BeforeEach
    void setUp() {
        sut = new PlaylistResource();
    }

    @Test
    void allPlaylistsSuccess() {
        String token = "1234";
        Response actualResult = sut.getAllPlaylists(token);
        assertEquals(Response.Status.OK.getStatusCode(), actualResult.getStatus());
    }

    @Test
    void allPlaylistsFail() {
        String token = "12345";
        Response actualResult = sut.getAllPlaylists(token);
        ErrorDTO errorDTO = (ErrorDTO) actualResult.getEntity();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), actualResult.getStatus());
        assertEquals("Invalid token.", errorDTO.getMessage());
    }
}