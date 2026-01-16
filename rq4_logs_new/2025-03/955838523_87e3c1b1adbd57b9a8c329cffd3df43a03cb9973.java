package a4.KU_TY_backend.KU_TY_backend.features;

import a4.KU_TY_backend.KU_TY_backend.controller.EventController;
import a4.KU_TY_backend.KU_TY_backend.controller.LoginController;
import a4.KU_TY_backend.KU_TY_backend.controller.UserController;
import a4.KU_TY_backend.KU_TY_backend.entity.Event;
import a4.KU_TY_backend.KU_TY_backend.exception.NotFoundException;
import a4.KU_TY_backend.KU_TY_backend.repository.EventRepository;
import a4.KU_TY_backend.KU_TY_backend.request.CreateEventRequest;
import a4.KU_TY_backend.KU_TY_backend.request.JoinEventRequest;
import a4.KU_TY_backend.KU_TY_backend.request.KickFromEventRequest;
import a4.KU_TY_backend.KU_TY_backend.request.LoginRequest;
import a4.KU_TY_backend.KU_TY_backend.response.EventResponse;
import a4.KU_TY_backend.KU_TY_backend.response.UserResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@Transactional
public class KickParticipantsTest {
    @Autowired
    private LoginController loginController;
    @Autowired
    private UserController userController;
    @Autowired
    private EventController eventController;
    @Autowired
    private EventRepository eventRepository;
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

        System.out.println("owner = " + ownerId + "part + " + participantId);

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
    }
    @Test
    void TheMessageShouldNotRevealSensitiveTechnicalDetails(){
        KickFromEventRequest kickFromEventRequest = new KickFromEventRequest();
        kickFromEventRequest.setParticipantId(participantId);
        kickFromEventRequest.setOwnerId(ownerId);
        kickFromEventRequest.setEventId(eventId);
        response = userController.kickFromEvent(kickFromEventRequest);
        assertNull(((Map<String, Object>)response.getBody()).get("data"));
    }
    @Test
    void IfTheOrganizerAttemptsToKickAParticipantWhoIsNoLongerInTheMeetingOrIsOtherwiseInvalid(){
        KickFromEventRequest kickFromEventRequest = new KickFromEventRequest();
        kickFromEventRequest.setParticipantId(UUID.randomUUID());
        kickFromEventRequest.setOwnerId(ownerId);
        kickFromEventRequest.setEventId(eventId);
        assertThrows(NotFoundException.class, () -> {
            userController.kickFromEvent(kickFromEventRequest);
        });
    }
}