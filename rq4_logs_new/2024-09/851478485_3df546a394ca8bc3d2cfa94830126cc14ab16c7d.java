package org.detective.controller.Request;

import org.detective.dto.RequestDTO;
import org.detective.services.Request.RequestService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/request")
public class RequestController {

    private RequestService requestService;

    public RequestController(RequestService requestService) {
        this.requestService = requestService;
    }

    @PostMapping("")
    public ResponseEntity<String> createRequest(@RequestBody RequestDTO requestDTO) {
        System.err.println("Controller : createRequest 실행");
        System.err.println("Controller RequestDTO : "+requestDTO);
        try {
            System.err.println("Controller : Try createRequest");
            requestService.createRequest(requestDTO);
            return ResponseEntity.ok("Request Created");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}