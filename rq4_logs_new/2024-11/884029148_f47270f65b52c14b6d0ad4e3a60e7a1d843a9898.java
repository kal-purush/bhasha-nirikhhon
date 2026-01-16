package entity;

import java.time.LocalDate;

/**
 * A simple implementation of the Ingredient interface.
 */
public class CommonIngredient implements Ingredient{

    private final String name;
    private final LocalDate expiryDate;

    public CommonIngredient(String name, LocalDate expiryDate) {
        this.name = name;
        this.expiryDate = expiryDate;
    }

    @Override
    public String getName() {return name;}

    @Override
    public LocalDate getExpiryDate() {return expiryDate;}

}