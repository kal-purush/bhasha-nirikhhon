package ch.uzh.ifi.hase.soprafs23.controller;

import ch.uzh.ifi.hase.soprafs23.constant.UserStatus;
import ch.uzh.ifi.hase.soprafs23.entity.GameRoom;
import ch.uzh.ifi.hase.soprafs23.entity.RoomCoordinator;
import ch.uzh.ifi.hase.soprafs23.entity.User;
import ch.uzh.ifi.hase.soprafs23.repository.UserRepository;
import ch.uzh.ifi.hase.soprafs23.rest.dto.GameRoomGetDTO;
import ch.uzh.ifi.hase.soprafs23.rest.dto.GameRoomPostDTO;
import ch.uzh.ifi.hase.soprafs23.rest.dto.GameRoomPutDTO;
import ch.uzh.ifi.hase.soprafs23.rest.dto.TopicGetDTO;
import ch.uzh.ifi.hase.soprafs23.service.ApiService;
import ch.uzh.ifi.hase.soprafs23.service.UserService;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GameRoomControllerIntegrationTests {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ApiService apiService;

    @Autowired
    UserRepository userRepository;

    @Autowired
    private UserService userService;

    private User user;

    @BeforeEach
    public void setup(){
        user = new User();
        user.setId(1L);
        user.setUsername("testUsername");
        user.setPassword("testPassword");
        user.setToken("1");
        user.setStatus(UserStatus.ONLINE);

        GameRoomPostDTO gameRoomPostDTO = new GameRoomPostDTO();
        gameRoomPostDTO.setLeaderId(1L);
        gameRoomPostDTO.setQuestionTopic("Geography");
        gameRoomPostDTO.setGameMode("Basic");
        gameRoomPostDTO.setNumOfQuestions(5);
        gameRoomPostDTO.setQuestionTopicId(3);

        userRepository.deleteAll();
    }

    @AfterEach
    public void clean(){
        userRepository.deleteAll();
    }

    @Test
    public void testGetTopics() throws Exception {
        List<TopicGetDTO> topics = apiService.getTopicList();

        mockMvc.perform(MockMvcRequestBuilders
                        .get("/categories")
                        .header("Authorization", "Bearer " + "top-secret-token")
                ).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].topicName", is(topics.get(0).getTopicName())));
    }

    @Test
    public void testCreateGameRoom() throws Exception {
        userService.createUser(user);
        //User savedUser = userRepository.save(user);

        GameRoomPostDTO gameRoomPostDTO = new GameRoomPostDTO();
        gameRoomPostDTO.setLeaderId(userRepository.findByUsername("testUsername").getId());
        gameRoomPostDTO.setQuestionTopic("Geography");
        gameRoomPostDTO.setGameMode("Basic");
        gameRoomPostDTO.setNumOfQuestions(5);
        gameRoomPostDTO.setQuestionTopicId(3);

        MockHttpServletRequestBuilder postRequest = post("/createRoom")
                .header("Authorization", "Bearer " + "top-secret-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(asJsonString(gameRoomPostDTO));

        mockMvc.perform(postRequest).andExpect(jsonPath("$.leader.username", is(user.getUsername())));
    }

    @Test
    public void testJoinGameRoom() throws Exception {
        User secondUser = new User();
        secondUser.setUsername("second");
        secondUser.setPassword("pw2");

        userService.createUser(secondUser);

        GameRoomPutDTO gameRoomPutDTO = new GameRoomPutDTO();
        gameRoomPutDTO.setRoomCode("123456");
        gameRoomPutDTO.setUserId(userRepository.findByUsername("second").getId().intValue());

        GameRoom gameRoom = new GameRoom();
        gameRoom.setRoomCode("123456");
        gameRoom.setLeader(user);
        gameRoom.getMembers().put(user.getId(),user);

        RoomCoordinator roomCoordinator = RoomCoordinator.getInstance();
        roomCoordinator.addRoom(gameRoom);

        MockHttpServletRequestBuilder putRequest = put("/joinRoom")
                .header("Authorization", "Bearer " + "top-secret-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(asJsonString(gameRoomPutDTO));

        mockMvc.perform(putRequest).andExpect(jsonPath("$.members[1].username", is(secondUser.getUsername())));
    }



    private String asJsonString(final Object object) {
        try {
            return new ObjectMapper().writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    String.format("The request body could not be created.%s", e.toString()));
        }
    }
}