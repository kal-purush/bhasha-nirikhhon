package com.gitlab.tomaszgryczka.fungiseeker.application.search;


import com.gitlab.tomaszgryczka.fungiseeker.application.dtos.PlaceSearchDTO;
import com.gitlab.tomaszgryczka.fungiseeker.infrastructure.place.PlaceSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RequestMapping("/api/place-search")
@RestController
public class PlaceSearchController {

    private final PlaceSearchService placeSearchService;

    @GetMapping("/{distance}/{longitude}/{latitude}")
    public PlaceSearchDTO getBestPlaceForMushroomHunting(@PathVariable Integer distance,
                                                         @PathVariable float longitude,
                                                         @PathVariable float latitude) {

        return placeSearchService.getBestPlaceForMushroomHunting(distance, longitude, latitude);
    }
}