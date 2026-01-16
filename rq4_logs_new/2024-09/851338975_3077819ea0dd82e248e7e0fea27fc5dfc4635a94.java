package com.reservibe.application.controller.restaurant.search;

import com.reservibe.application.response.GenericResponse;
import com.reservibe.application.response.PresenterResponse;
import com.reservibe.domain.output.restaurant.RestaurantListOutput;
import com.reservibe.domain.presenters.restaurant.RestaurantListPresenter;
import com.reservibe.domain.usecase.restaurant.search.SearchRestaurantByCityUseCase;
import com.reservibe.infra.adapter.restaurant.SearchRestaurantService;
import com.reservibe.infra.repository.restaurant.RestaurantRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/restaurant/search/city")
public class SearchRestaurantByCityController {

    private final RestaurantRepository restaurantRepository;

    public SearchRestaurantByCityController(RestaurantRepository restaurantRepository) {
        this.restaurantRepository = restaurantRepository;
    }

    @GetMapping("/{city}")
    public ResponseEntity<Object> searchRestaurantByName(@PathVariable String city) {
        SearchRestaurantByCityUseCase searchRestaurantByCityUseCase = new SearchRestaurantByCityUseCase(new SearchRestaurantService(restaurantRepository));
        RestaurantListOutput output = searchRestaurantByCityUseCase.execute(city);
        if (output.getOutputStatus().getCode() != 200) {
            return new GenericResponse().response(output);
        }
        RestaurantListPresenter restaurantPresenter = new RestaurantListPresenter(output);
        return new PresenterResponse().response(restaurantPresenter);
    }
}