package com.ichurch.backend.controller;

import com.ichurch.backend.service.FileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping(value = "/api/image")
public class FileController {

    @Autowired
    private FileService fileService;

    @GetMapping(value = "/{imageUrl}")
    public ResponseEntity<?> getEventImage(@PathVariable() String imageUrl,
                                           @RequestParam(value = "w", defaultValue = "62")int targetWidth,
                                           @RequestParam(value = "h", defaultValue = "62")int targetHeight) throws IOException {

        return ResponseEntity.status(HttpStatus.OK).body(fileService.getEncodedImage(imageUrl, targetWidth, targetHeight));
    }}