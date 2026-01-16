package ch.volleymanager.repository;


import ch.volleymanager.AbstractTest;
import ch.volleymanager.domain.Event;
import ch.volleymanager.repo.EventRepo;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EventRepoTest extends AbstractTest {
    @Autowired
    EventRepo eventRepo;


    private Long eventId;
    private Event newEvent;

    @Override
    @BeforeEach
    public void setUp(){
        String jsonEvent = readResource(new ClassPathResource("event/Event.json"));
        try{
            newEvent = mapFromJson(jsonEvent, Event.class);
           List<Event> events = eventRepo.findEventByEventName(newEvent.getEventName());
           if(events.isEmpty()){
            newEvent = eventRepo.save(newEvent);
            eventId = newEvent.getEventId();
           }else{
               newEvent = events.get(0);
               eventId = events.get(0).getEventId();
           }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @AfterEach
    public void tearDown(){
        }
    @Test
    public void showAllAssignedTeams() throws Exception{
        List <Event> events = eventRepo.findAll();
        assertTrue(events.size()>0);
    }

    @Test public void shouldHaveAtLeasOneEagerContact()throws Exception{
        List<Event> events = eventRepo.findAllWithEagerRelationships();
        assertTrue(events.size()>0);
    }

    @Test public void shouldLoadOneEagerContact() throws  Exception{
        eventRepo.findByIdWithEagerRelationships(eventId).ifPresent(event -> {
            try {
                assertEquals(mapToJson(newEvent), mapToJson(event));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }


}