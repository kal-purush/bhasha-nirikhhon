package com.hackathon.TwojaPomocBackend.controller;

import com.hackathon.TwojaPomocBackend.model.Request;
import com.hackathon.TwojaPomocBackend.model.TestRequest;
import com.hackathon.TwojaPomocBackend.service.RequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

@RestController
@RequiredArgsConstructor
public class RequestController {

    private final RequestService requestService;

    @RequestMapping(path = "/requests", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Request> create(@RequestBody Request request) {
        Request newRequest = requestService.create(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(newRequest);
    }

    @RequestMapping(path = "/requests", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Request>> getAll(){
        List<Request> requests =  requestService.getAllRequests();
        return requests.isEmpty()
                ? ResponseEntity.status(HttpStatus.NOT_FOUND).build() : ResponseEntity.ok().body(requests);
    }

}