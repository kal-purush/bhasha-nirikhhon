package com.reservibe.application.controller.restaurant.search;

import com.reservibe.application.response.GenericResponse;
import com.reservibe.application.response.PresenterResponse;
import com.reservibe.domain.entity.restaurant.Address;
import com.reservibe.domain.output.restaurant.RestaurantListOutput;
import com.reservibe.domain.presenters.restaurant.RestaurantListPresenter;
import com.reservibe.domain.usecase.restaurant.search.SearchRestaurantByAddressUseCase;
import com.reservibe.infra.adapter.restaurant.SearchRestaurantService;
import com.reservibe.infra.repository.restaurant.RestaurantRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/restaurant/search/address")
public class SearchRestaurantByAddressController {

    private final RestaurantRepository restaurantRepository;

    public SearchRestaurantByAddressController(RestaurantRepository restaurantRepository) {
        this.restaurantRepository = restaurantRepository;
    }

    @PostMapping
    public ResponseEntity<Object> searchRestaurantByAddress(@RequestBody Address address) {
        SearchRestaurantByAddressUseCase searchRestaurantByAddressUseCase = new SearchRestaurantByAddressUseCase(new SearchRestaurantService(restaurantRepository));
        RestaurantListOutput output = searchRestaurantByAddressUseCase.execute(address);
        if (output.getOutputStatus().getCode() != 200) {
            return new GenericResponse().response(output);
        }
        RestaurantListPresenter listPresenter = new RestaurantListPresenter(output);
        return new PresenterResponse().response(listPresenter);
    }
}