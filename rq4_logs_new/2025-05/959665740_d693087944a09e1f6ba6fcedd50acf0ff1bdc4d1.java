package com.example.backend.controllers;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;


@RestController
@RequestMapping("/image")
@RequiredArgsConstructor
public class ImageController {
    @GetMapping("/residents/{filename}")
    public ResponseEntity<Resource> getImage(@PathVariable String filename) throws IOException {
        Resource resource = new ClassPathResource("static/images/residents/" + filename);

        if (resource.exists() && resource.isReadable()) {
            return ResponseEntity.ok()
                    .contentType(MediaType.IMAGE_JPEG) // Hoặc detect đúng kiểu file (xem phần note dưới)
                    .body(resource);
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}