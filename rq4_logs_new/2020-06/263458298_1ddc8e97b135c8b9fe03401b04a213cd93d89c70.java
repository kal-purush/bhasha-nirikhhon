package com.xml.controller;

import com.xml.dto.RegistrationRequestDto;
import com.xml.dto.UserDto;
import com.xml.model.User;
import com.xml.service.RegistrationRequestService;
import com.xml.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin(value = "http://localhost:4200")
@RequestMapping(value = "/api/registrationRequest", produces = MediaType.APPLICATION_JSON_VALUE)
public class RegistrationRequestController {

    @Autowired
    private RegistrationRequestService registrationRequestService;

    @Autowired
    private UserService userService;


    @GetMapping(value = "/all")
    public ResponseEntity<List<RegistrationRequestDto>> getAll(){
        try{
            List<RegistrationRequestDto> requestDtos = this.registrationRequestService.getAll();
            return new ResponseEntity<>(requestDtos, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

    @DeleteMapping(value = "/delete/{id}")
    public ResponseEntity<?> deleteRegistrationRequest(@PathVariable Long id){
        try {
            this.registrationRequestService.deleteRegistrationRequest(id);
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

    @PostMapping(value = "/confirm")
    public ResponseEntity<?> confirmRegistrationRequest(@RequestBody RegistrationRequestDto requestDto){
        try {
            User newCustomer = userService.createCustomerFromRequest(requestDto);
            registrationRequestService.deleteRegistrationRequest(requestDto.getId());
            userService.saveCustomer(newCustomer);
            return new ResponseEntity<>(HttpStatus.OK);

        } catch (Exception e){
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }
}