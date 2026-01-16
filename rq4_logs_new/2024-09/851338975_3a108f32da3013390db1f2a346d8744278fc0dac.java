package com.reservibe.domain.input.restaurant;

import com.reservibe.domain.enums.retaurant.Cuisine;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public record RestaurantInput(
        @NotBlank(message = "The restaurant name can't be null or empty") String name,
        @NotBlank(message = "The restaurant address can't be null or empty")String address,
        @NotBlank(message = "The restaurant phone can't be null or empty")String phoneNumber,
        @NotBlank(message = "The restaurant description can't be null or empty")String description,
        @NotNull Cuisine cuisine,
        @NotNull List<OpeningHoursInput> openingHours
) {

    public RestaurantInput(String name, String address, String phoneNumber, String description, Cuisine cuisine) {
        this(name, address, phoneNumber, description, cuisine, new ArrayList<>());
    }
}