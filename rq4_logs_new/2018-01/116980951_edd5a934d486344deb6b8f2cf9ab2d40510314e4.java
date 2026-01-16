package com.lohika.morning.ecs.domain.event;

import com.vaadin.annotations.Theme;
import com.vaadin.server.VaadinRequest;
import com.vaadin.spring.annotation.SpringUI;
import com.vaadin.ui.Button;
import com.vaadin.ui.Grid;
import com.vaadin.ui.Notification;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

@SpringUI(path = "/events")
@Theme("valo")
public class EventUi extends UI {
    private EventRepository eventRepository;
    private Grid<com.lohika.morning.ecs.domain.event.Event> grid;

    @Autowired
    public EventUi(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
        this.grid = new Grid<>(com.lohika.morning.ecs.domain.event.Event.class);
        grid.setSizeFull();
        grid.getColumn("id").setHidden(true);
        grid.a
    }

    @Override
    protected void init(VaadinRequest vaadinRequest) {
        final VerticalLayout root = new VerticalLayout();
        root.setSizeFull();
//        root.setExpandRatio(grid, 1.0f);
        setContent(root);


        root.addComponents(grid);
        root.addComponent(new Button("Click me", e -> Notification.show("Hello Spring+Vaadin user!")));

        listCustomers();
    }

    private void listCustomers() {
        List<com.lohika.morning.ecs.domain.event.Event> events = StreamSupport.stream(eventRepository.findAll().spliterator(), false).collect(toList());
        grid.setItems(events);
    }
}