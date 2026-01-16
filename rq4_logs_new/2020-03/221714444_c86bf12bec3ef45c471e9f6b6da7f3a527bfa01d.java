package com.zakrzewski.intentionbook.controllers;

import com.zakrzewski.intentionbook.entities.CalendarApi;
import com.zakrzewski.intentionbook.finalVariables.URLStrings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.client.RestTemplate;


@Controller
public class LiturgicalCalendar {

    private RestTemplate restTemplate;

    @Autowired
    public LiturgicalCalendar(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @RequestMapping(value = "/calendar-info")
    public String calendarPage(Model model) {
        CalendarApi calendarApi = restTemplate.getForObject(URLStrings.TODAY_URL, CalendarApi.class);
        CalendarApi calendarApiYesterday = restTemplate.getForObject(URLStrings.YESTERDAY_URL, CalendarApi.class);
        CalendarApi calendarApiTomorrow = restTemplate.getForObject(URLStrings.TOMORROW_URL, CalendarApi.class);
        model.addAttribute("calendarApi", calendarApi);
        model.addAttribute("calendarApiYesterday", calendarApiYesterday);
        model.addAttribute("calendarApiTomorrow", calendarApiTomorrow);
        return "calendar-info";
    }
}