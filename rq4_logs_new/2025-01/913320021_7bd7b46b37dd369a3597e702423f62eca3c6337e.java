package com.management.restaurant.DTO.restaurant;

import com.management.restaurant.models.restaurant.MenuRestaurant;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalTime;

@Getter
@Setter
public class RestaurantResponseDTO {
 private Long id;
 @NotNull
 private String name;
 @NotNull
 private String address;
 @NotNull
 private String phoneNumber;
 @NotNull
 private LocalTime openingHours;
 @NotNull
 private LocalTime closingHours;
 @NotNull
 private MenuResponseDTO menuRestaurant;

}