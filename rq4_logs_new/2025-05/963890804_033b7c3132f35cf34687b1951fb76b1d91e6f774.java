package com.example.MusicApp.controller;

import com.example.MusicApp.DTO.ArtistDTO;
import com.example.MusicApp.service.ArtistService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("/artist")
public class ArtistController {

    @Autowired
    private ArtistService artistService;

    @GetMapping("/top")
    public List<ArtistDTO> getTop10Artists() {
        return artistService.getTop10Artists();
    }
}