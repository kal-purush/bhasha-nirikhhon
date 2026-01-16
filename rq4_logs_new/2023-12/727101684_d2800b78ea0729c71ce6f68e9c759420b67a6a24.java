package com.example.demo.publicapi.api;

import com.example.demo.publicapi.service.PublicService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/public")
@CrossOrigin
public class PublicController {

    private final PublicService publicService;

    @GetMapping(path = "/{}" , produces = "application/json; charset=UTF-8")
    public ResponseEntity<?> getPublic(@PathVariable int pageNum){

        return null;
    }
}