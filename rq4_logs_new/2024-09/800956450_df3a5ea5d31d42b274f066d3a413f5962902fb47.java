package io.github.xpakx.images_cdc.data;

import io.github.xpakx.images_cdc.data.dto.EventData;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@CrossOrigin("http://localhost:8001")
public class EventController {
    private final EventService service;

    public EventController(EventService service) {
        this.service = service;
    }

    @GetMapping("/events/user/{id}")
    public List<EventData> getEvents(@PathVariable Long id) {
        return service.getEvents(id);
    }

    @GetMapping("/events")
    public List<EventData> getAllEvents() {
        return service.getAllEvents();
    }
}