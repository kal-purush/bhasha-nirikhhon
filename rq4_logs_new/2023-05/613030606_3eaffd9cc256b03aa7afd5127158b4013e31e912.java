package ch.uzh.ifi.hase.soprafs23.service;

import ch.uzh.ifi.hase.soprafs23.entity.GameRoom;
import ch.uzh.ifi.hase.soprafs23.entity.RoomCoordinator;
import ch.uzh.ifi.hase.soprafs23.entity.User;
import ch.uzh.ifi.hase.soprafs23.entity.Vote;
import ch.uzh.ifi.hase.soprafs23.exceptions.RoomNotFoundException;
import com.corundumstudio.socketio.SocketIOClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SocketServiceUnitTests {

    private User testUser1;
    private User testUser2;


    @Mock
    private SocketBasics socketBasics;
    @Mock
    private SocketIOClient socketIOClient;

    @Mock
    private GameRoomService gameRoomService;

    @InjectMocks
    private SocketService socketService;



    @BeforeEach
    public void setup() {
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
    public void testJoinRoom_UserIsNotLeader() throws Exception {
        GameRoom gameRoom = new GameRoom();
        gameRoom.setLeaderUserId(1);
        gameRoom.setRoomCode("123456");

        RoomCoordinator roomCoordinator = RoomCoordinator.getInstance();
        roomCoordinator.addRoom(gameRoom);

        socketService.joinRoom("123456",1,"bearerToken", socketIOClient);

        verify(gameRoomService.addPlayerToGameRoom(gameRoom, 1), times(1));
    }

    @Test
    public void testJoinRoom_RoomNotFound() throws Exception {
        GameRoom gameRoom = new GameRoom();
        gameRoom.setLeaderUserId(1);
        gameRoom.setRoomCode("123456");

        RoomCoordinator roomCoordinator = RoomCoordinator.getInstance();
        roomCoordinator.addRoom(gameRoom);

        RoomNotFoundException roomNotFoundException = assertThrows(RoomNotFoundException.class, () -> {
            socketService.joinRoom("123455",1,"bearerToken", socketIOClient);
        });
        assertEquals(roomNotFoundException.getMessage(), "Unable to find game room with code: " + "123455");
    }

    @Test
    public void testSendMemberToArray() throws Exception {
        GameRoom gameRoom = new GameRoom();
        gameRoom.setLeaderUserId(1);
        gameRoom.setRoomCode("123456");
        gameRoom.setMembers(List.of(testUser1));

        RoomCoordinator roomCoordinator = RoomCoordinator.getInstance();
        roomCoordinator.addRoom(gameRoom);

        socketService.sendMemberArray("123456", socketIOClient);

        //verify(socketBasics.sendObjectToRoom(any(), any(), any()), times(1));
        //"123456", EventNames.JOINED_PLAYERS.eventName, gameRoom.getMembers()
    }

    @Test
    public void testVotesListAsMap() throws Exception {
        Vote vote = new Vote();
        List<Vote> votes = List.of(vote);

        HashMap map = socketService.votesListAsMap(votes);
        assertEquals(1, map.size());
    }
}