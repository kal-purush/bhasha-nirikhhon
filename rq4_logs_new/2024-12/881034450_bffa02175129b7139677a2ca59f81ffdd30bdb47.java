package org.example.restaurantvoting.restaurant;

import org.example.restaurantvoting.MatcherFactory;
import org.example.restaurantvoting.restaurant.model.Dish;
import org.example.restaurantvoting.restaurant.to.DishTo;

public class DishTestData {

    public static final MatcherFactory.Matcher<Dish> DISH_MATCHER = MatcherFactory.usingIgnoringFieldsComparator(Dish.class, "restaurant");
    public static final MatcherFactory.Matcher<DishTo> DISH_TO_MATCHER = MatcherFactory.usingIgnoringFieldsComparator(DishTo.class, "restaurant");

    public static final int BURGER_ID = 1;
    public static final int MEATBALLS_ID = 2;
    public static final int NOT_FOUND = 100;

    public static final Dish burger = new Dish(BURGER_ID, "Burger", 9999, null);
    public static final DishTo burgerTo = DishUtil.createToFromDish(burger);
    public static final Dish meatballs = new Dish(MEATBALLS_ID, "Meatballs", 19900, null);
    public static final DishTo meatballsTo = DishUtil.createToFromDish(meatballs);

    public static Dish getNew() {
        return new Dish(null, "french fries", 1234, null);
    }

    public static DishTo getNewTo() {
        return DishUtil.createToFromDish(getNew());
    }

    public static Dish getUpdated() {
        return new Dish(BURGER_ID, "UpdatedName", 34560, null);
    }

    public static DishTo getUpdatedTo() {
        return DishUtil.createToFromDish(getUpdated());
    }

    public static DishTo getNewInvalidTo() {
        Dish dish = getNew();
        dish.setPrice(-1);
        return DishUtil.createToFromDish(dish);
    }

    public static DishTo getUpdatedInvalidTo() {
        Dish dish = getUpdated();
        dish.setName("");
        return DishUtil.createToFromDish(dish);
    }
}