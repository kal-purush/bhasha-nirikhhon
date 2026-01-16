package com.outdoor.foodcalc.domain.repository.dish;

import com.outdoor.foodcalc.domain.model.dish.Dish;

import java.util.List;
import java.util.Optional;

/**
 * Product repository responsible for {@link Dish} persistence.
 *
 * @author Olga Borovyk
 */
public interface IDishRepo {

    /**
     * Loads all {@link Dish}.
     *
     * @return list of dishes
     */
    List<Dish> getAllDishes();

    /**
     * Count dishes number in the dish category
     * @param categoryId dish category Id
     * @return dishes number
     */
    long countDishesInCategory(long categoryId);

    /**
     * Loads {@link Dish} object by Id.
     *
     * @param id dish id
     * @return loaded dish
     */
    Optional<Dish> getDish(long id);

    /**
     * Add new {@link Dish}.
     *
     * @param dish to add
     * @return auto generated Id
     */
    long addDish(Dish dish);

    /**
     * Updates selected {@link Dish} with new value.
     *
     * @param dish to update
     * @return if dish was updated
     */
    boolean updateDish(Dish dish);

    /**
     * Deletes selected {@link Dish}.
     *
     * @param id dish to delete
     * @return if dish was deleted
     */
    boolean deleteDish(long id);

    /**
     * Check if specified {@link Dish} exists.
     *
     * @param id dish to check
     * @return if dish exists
     */
    boolean existsDish(long id);
}