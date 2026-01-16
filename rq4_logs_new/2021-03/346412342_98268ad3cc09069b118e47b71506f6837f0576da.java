package com.hackner.musicompass.controller;

import com.hackner.musicompass.model.ArtistInfo;
import com.hackner.musicompass.service.ArtistInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("api/artistsearch/")
public class ArtistSearchController {

    private final ArtistInfoService artistInfoService;

    @Autowired
    public ArtistSearchController(ArtistInfoService artistInfoService) {
        this.artistInfoService = artistInfoService;
    }

    @GetMapping("{searchTerm}")
    public List<ArtistInfo> findArtist(@PathVariable String searchTerm){
        return artistInfoService.getArtistInfoListBySearchTerm(searchTerm);
    }
}