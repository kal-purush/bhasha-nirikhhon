package com.svan001.foodtruck.bootstrap;

import com.svan001.foodtruck.domain.FoodItem;
import com.svan001.foodtruck.repository.FoodItemRepository;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
public class DevBootstrap implements ApplicationListener<ContextRefreshedEvent> {

    private FoodItemRepository foodItemRepository;

    public DevBootstrap(FoodItemRepository foodItemRepository) {
        this.foodItemRepository = foodItemRepository;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        FoodItem burger = new FoodItem("Burger", 9.50);

        foodItemRepository.save(burger);

    }
}