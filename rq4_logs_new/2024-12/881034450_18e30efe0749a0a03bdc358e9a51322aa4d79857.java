package org.example.restaurantvoting.restaurant;

import lombok.experimental.UtilityClass;
import org.example.restaurantvoting.restaurant.model.Dish;
import org.example.restaurantvoting.restaurant.to.DishTo;

import java.math.BigDecimal;
import java.math.RoundingMode;

@UtilityClass
public class DishUtil {

    public static Dish createNewDishFromTo(DishTo dishTo) {
        long price = dishTo.getPrice().multiply(BigDecimal.valueOf(100)).longValue();
        return new Dish(null, dishTo.getName(), price, dishTo.getRestaurant());
    }

    public static DishTo createToFromDish(Dish dish) {
        BigDecimal price = BigDecimal.valueOf(dish.getPrice()).divide(BigDecimal.valueOf(100)).setScale(2, RoundingMode.HALF_UP);
        return new DishTo(dish.getId(), dish.getName(), price, dish.getRestaurant());
    }
}