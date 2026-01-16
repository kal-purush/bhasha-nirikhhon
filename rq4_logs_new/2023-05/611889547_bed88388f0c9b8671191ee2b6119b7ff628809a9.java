package com.luximed.client.controller;

import com.luximed.client.appointment.ClientDatabase;
import com.luximed.client.dto.PersonalDataDto;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping(value = "api/client/personalData")
public class PersonalDataController {
    private final ClientDatabase client;

    public PersonalDataController(ClientDatabase client) {
        this.client = client;
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/update")
    public ResponseEntity<String> updatePersonalData(@RequestBody PersonalDataDto personalDataDto) {
        return client.updatePersonalData(personalDataDto);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/delete")
    public ResponseEntity<String> deletePersonalData(@RequestParam String pesel) {
        return client.deletePersonaldata(pesel);
    }
}