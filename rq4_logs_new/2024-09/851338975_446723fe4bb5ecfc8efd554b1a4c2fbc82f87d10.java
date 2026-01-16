package com.reservibe.application.controller.restaurant.search;

import com.reservibe.domain.output.restaurant.RestaurantOutput;
import com.reservibe.infra.repository.restaurant.RestaurantRepository;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/restaurant/search/name")
public class SearchRestaurantByNameController {

    private final RestaurantRepository restaurantRepository;

    public SearchRestaurantByNameController(RestaurantRepository restaurantRepository) {
        this.restaurantRepository = restaurantRepository;
    }

    @GetMapping("/{name}")
    public RestaurantOutput searchRestaurantByName(@PathVariable String name) {

    }
}