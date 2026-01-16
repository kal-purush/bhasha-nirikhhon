package ch.volleymanager.resource;
import ch.volleymanager.domain.Event;
import ch.volleymanager.service.EventService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

public class EventResource {
    private final EventService eventService;

    public EventResource(EventService eventService){
        this.eventService = eventService;
    }

    @GetMapping ("/all")
    public ResponseEntity<List<Event>>getAllEvents(){
        List<Event> events = eventService.findAllEvents();
        return new ResponseEntity<>(events, HttpStatus.OK);
    }

    @GetMapping ("/find/{eventId}")
    public ResponseEntity<Event> addEvent (@RequestBody Event event){
        Event newEvent = eventService.addEvent(event);
        return new ResponseEntity<>(newEvent, HttpStatus.CREATED);
    }

    @GetMapping ("/update")
    public ResponseEntity<?>deleteEvent(@PathVariable("eventId")Long eventId){
        eventService.deleteEventById(eventId);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}