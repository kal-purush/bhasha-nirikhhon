package a4.KU_TY_backend.KU_TY_backend.features;

import a4.KU_TY_backend.KU_TY_backend.controller.EventController;
import a4.KU_TY_backend.KU_TY_backend.controller.FeedbackController;
import a4.KU_TY_backend.KU_TY_backend.controller.LoginController;
import a4.KU_TY_backend.KU_TY_backend.controller.UserController;
import a4.KU_TY_backend.KU_TY_backend.entity.Event;
import a4.KU_TY_backend.KU_TY_backend.repository.EventRepository;
import a4.KU_TY_backend.KU_TY_backend.request.CreateEventRequest;
import a4.KU_TY_backend.KU_TY_backend.request.GiveFeedbackRequest;
import a4.KU_TY_backend.KU_TY_backend.request.JoinEventRequest;
import a4.KU_TY_backend.KU_TY_backend.request.LoginRequest;
import a4.KU_TY_backend.KU_TY_backend.response.EventResponse;
import a4.KU_TY_backend.KU_TY_backend.response.FeedbackResponse;
import a4.KU_TY_backend.KU_TY_backend.response.UserResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
@ActiveProfiles("test")
public class FeedbackCollectionTest {
    @Autowired
    private LoginController loginController;
    @Autowired
    private UserController userController;
    @Autowired
    private EventController eventController;
    @Autowired
    private EventRepository eventRepository;
    @Autowired
    private FeedbackController feedbackController;
    private ResponseEntity<Object> response;
    private UUID ownerId;
    private UUID eventId;
    private UUID participantId;

    @BeforeEach
    void setUp(){
        LoginRequest loginRequest = new LoginRequest("b6510405491", "25698720@Jj");
        response = loginController.login(loginRequest);
        ownerId = ((UserResponse) ((Map<String, Object>) response.getBody()).get("data")).getUserId();


        loginRequest = new LoginRequest("b6510405351", "210746_Ttt");
        response = loginController.login(loginRequest);
        participantId = ((UserResponse) ((Map<String, Object>) response.getBody()).get("data")).getUserId();


        CreateEventRequest request = new CreateEventRequest();
        request.setCapacity(20);
        request.setName("AHAHAH");
        request.setCreatedBy(ownerId);
        request.setStartDate(LocalDate.now());
        request.setStartTime(LocalTime.now());
        request.setLocation("HAHA");
        request.setTypeName("TEST");
        response = eventController.create(request);
        eventId = ((EventResponse) ((Map<String, Object>) response.getBody()).get("data")).getEventId();
        Event event = eventRepository.findById(eventId).get();


        JoinEventRequest joinEventRequest = new JoinEventRequest();
        joinEventRequest.setEventId(eventId);
        joinEventRequest.setUserId(participantId);
        userController.joinEvent(joinEventRequest);

        GiveFeedbackRequest giveFeedbackRequest = new GiveFeedbackRequest();
        giveFeedbackRequest.setFeedback("สนุกมาก");
        giveFeedbackRequest.setUserId(null);
        giveFeedbackRequest.setEventId(eventId);
        feedbackController.giveFeedback(giveFeedbackRequest);
    }
    @Test
    void UserIdentityCannotBeDeterminedFromAnonymousFeedback(){
        response = feedbackController.getFeedbackFromEvent(eventId);
        Map<String, Object> body = (Map<String, Object>)response.getBody();
        assertNull(((FeedbackResponse)((ArrayList<FeedbackResponse>)body.get("data")).get(0)).getUserId() );

    }
}