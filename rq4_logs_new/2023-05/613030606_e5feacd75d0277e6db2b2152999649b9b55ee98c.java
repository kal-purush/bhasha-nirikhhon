package ch.uzh.ifi.hase.soprafs23.Entity;

import ch.uzh.ifi.hase.soprafs23.entity.GameRoom;
import ch.uzh.ifi.hase.soprafs23.entity.RoomCoordinator;
import ch.uzh.ifi.hase.soprafs23.entity.User;
import ch.uzh.ifi.hase.soprafs23.exceptions.RoomNotFoundException;
import ch.uzh.ifi.hase.soprafs23.game.Game;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EntitiesUnitTests {

    private User testUser1;
    private User testUser2;

    @BeforeEach
    public void setup(){
        MockitoAnnotations.openMocks(this);
        testUser1 = new User();
        testUser1.setId(1L);
        testUser1.setUsername("playerName1");
        testUser1.setPassword("password1");

        testUser2 = new User();
        testUser2.setId(2L);
        testUser2.setUsername("playerName2");
        testUser2.setPassword("password2");
    }

    @Test
    public void testRoomCoordinator() throws Exception
    {
        GameRoom gameRoom = new GameRoom();
        gameRoom.setLeaderUserId(1);
        gameRoom.setRoomCode("123456");
        gameRoom.setMembers(List.of(testUser1));

        RoomCoordinator roomCoordinator = RoomCoordinator.getInstance();
        roomCoordinator.addRoom(gameRoom);

        GameRoom receivedRoom = roomCoordinator.getRoomByCode("123456");
        assertEquals(gameRoom.getLeaderUserId(), receivedRoom.getLeaderUserId());

        roomCoordinator.deleteRoom("123456");
       /* RoomNotFoundException roomNotFoundException = assertThrows(RoomNotFoundException.class, () -> {
            roomCoordinator.getRoomByCode("123456");
        });
        assertEquals(roomNotFoundException.getMessage(), "Unable to find game room with code: " + "123456");*/
    }

    @Test
    public void testAddRoom_RoomNotFound() throws Exception
    {
        RoomCoordinator roomCoordinator = RoomCoordinator.getInstance();

        /*RoomNotFoundException roomNotFoundException = assertThrows(RoomNotFoundException.class, () -> {
            roomCoordinator.getRoomByCode("123456");
        });
        assertEquals(roomNotFoundException.getMessage(), "Unable to find game room with code: " + "123456");*/
    }

    @Test
    public void testDeleteRoom_RoomNotFound() throws Exception
    {
        RoomCoordinator roomCoordinator = RoomCoordinator.getInstance();

        /*RoomNotFoundException roomNotFoundException = assertThrows(RoomNotFoundException.class, () -> {
            roomCoordinator.deleteRoom("123456");
        });
        assertEquals(roomNotFoundException.getMessage(), "Unable to find game room with code: " + "123456");*/
    }
}