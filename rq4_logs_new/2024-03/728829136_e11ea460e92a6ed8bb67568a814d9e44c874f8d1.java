package com.quokkatech.foodtruckmanagement.unitTests;

import com.quokkatech.foodtruckmanagement.application.services.FoodTruckService;
import com.quokkatech.foodtruckmanagement.domain.entities.FoodTruck;
import com.quokkatech.foodtruckmanagement.domain.entities.User;
import com.quokkatech.foodtruckmanagement.domain.repositories.FoodTruckRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.TestComponent;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FoodTruckServiceTest {
    @Mock
    private FoodTruckRepository foodTruckRepository;
    @InjectMocks
    private FoodTruckService foodTruckService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void createFoodTruck(){
        FoodTruck foodTruck = new FoodTruck(1L,"FoodTruck1", new User(1L,"user","password","OWNER", "TestCompany"));

        foodTruckService.createFoodTruck(foodTruck);

        verify(foodTruckRepository, times(1)).save(foodTruck);
    }

}