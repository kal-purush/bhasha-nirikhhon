package com.example.librarymanagement.controller;

import com.example.librarymanagement.dto.PublisherDto;
import com.example.librarymanagement.dto.mapper.PublisherMapper;
import com.example.librarymanagement.service.PublisherService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/publisher")
public class PublisherController {
    private final PublisherService publisherService;

    public PublisherController(PublisherService publisherService) {
        this.publisherService = publisherService;
    }

    @GetMapping("/all")
    public ResponseEntity<List<PublisherDto>> getAllPublishers(){
        return new ResponseEntity<>(PublisherMapper.toPublishers(publisherService.getAllPublishers()), HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PublisherDto> getPublisherById(@PathVariable Long id){
        return new ResponseEntity<>(new PublisherDto(publisherService.getPublisherById(id)), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<PublisherDto> createPublisher(@RequestBody PublisherDto publisherDto){
        return new ResponseEntity<>(
                new PublisherDto(publisherService.createPublisher(PublisherMapper.toPublisher(publisherDto))),
                HttpStatus.CREATED);
    }

    @PutMapping("/{id}")
    public ResponseEntity<PublisherDto> updatePublisher(@PathVariable Long id, @RequestBody PublisherDto publisherDto){
        return new ResponseEntity<>(
                new PublisherDto(publisherService.updatePublisher(id, PublisherMapper.toPublisher(publisherDto))),
                HttpStatus.OK);
    }
}