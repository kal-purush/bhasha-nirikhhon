package com.management.restaurant.services;

import com.management.restaurant.models.restaurant.Dish;
import com.management.restaurant.models.restaurant.MenuRestaurant;
import com.management.restaurant.repositories.DishRepository;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DishService {
  private DishRepository repository;

  @Autowired
  public DishService(DishRepository repository) {
    this.repository = repository;
  }
  public List<Dish> getAllDish(){
    return repository.findAll();
  }
}