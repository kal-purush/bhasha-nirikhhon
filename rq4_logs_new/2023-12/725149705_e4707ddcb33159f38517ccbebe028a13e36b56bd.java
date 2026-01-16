package com.spring.boot.controller;

import com.spring.boot.MemberOption;
import com.spring.boot.dto.EventDTO;
import com.spring.boot.dto.MemberDTO;
import com.spring.boot.service.EventService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@Controller
public class EventController {

    @Autowired
    private EventService eventService;

    @RequestMapping(value = "/eventManager")
    public String eventManager(HttpServletRequest request) throws Exception{
        List<EventDTO> events = eventService.findAll();
        System.out.println(events);
        request.setAttribute("events", events);

        return "eventManager";
    }

    @PostMapping(value = "/eventRegisterAction")
    public String eventRegisterAction(EventDTO event, HttpServletRequest request) throws Exception{
        eventService.save(event);
        return eventManager(request);
    }

    @RequestMapping(value = "/eventDeleteAction")
    public String eventDeleteAction(@RequestParam("id") int id, HttpServletRequest request) throws Exception {
        eventService.delete(id);
        return eventManager(request);
    }


    @RequestMapping(value = "/eventSearch")
    public String memberSearch(String searchDivide, String searchContent, HttpServletRequest request) throws Exception{
        if (searchContent.isEmpty()) { return eventManager(request); }

        System.out.println(searchContent + " " + searchDivide);
        List<EventDTO> events = eventService.findByOption(searchDivide, searchContent);
        request.setAttribute("events", events);

        return "eventManager";
    }

}