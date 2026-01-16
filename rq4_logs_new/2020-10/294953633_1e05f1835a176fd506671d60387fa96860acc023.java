package jimmy.mcgymmy.model.food;

import jimmy.mcgymmy.commons.util.AppUtil;
import jimmy.mcgymmy.commons.util.CollectionUtil;
import jimmy.mcgymmy.model.person.Person;

/**
 * Represents a Food item in McGymmy.
 */
public class Food {
    public static final String FOOD_NAME_MESSAGE_CONTRAINT = "Food name can take in any value, and it cannot be blank";

    /*
     * The first character of name must not be a whitespace,
     * otherwise " " (a blank string) becomes a valid input.
     */
    public static final String VALIDATION_REGEX = "[^\\s].*";

    // Identity field names
    private final String name;
    private final Protein protein;
    private final Carbohydrate carbs;
    private final Fat fat;

    public Food(String name, Protein protein, Fat fat, Carbohydrate carbs) {
        CollectionUtil.requireAllNonNull(name, protein, carbs, fat);
        AppUtil.checkArgument(isValidName(name), FOOD_NAME_MESSAGE_CONTRAINT);
        this.name = name;
        this.protein = protein;
        this.carbs = carbs;
        this.fat = fat;
    }

    // Constructor for convenience

    /**
     * Every field must be present and not null.
     * A Constructor made for convenience
     */
    public Food(String name, int proteinAmount, int fatAmount, int carbsAmount) {
        this(name, new Protein(proteinAmount), new Fat(fatAmount), new Carbohydrate(carbsAmount));
    }

    private boolean isValidName(String test) {
        return test.matches(VALIDATION_REGEX);
    }

    // getters : make when needed
    public String getName() {
        return this.name;
    }

    public Protein getProtein() {
        return this.protein;
    }

    public Carbohydrate getCarbs() {
        return this.carbs;
    }

    public Fat getFat() {
        return this.fat;
    }

    public boolean isSameFood(Food otherFood) {
        return this.equals(otherFood);
    }

    @Override
    public boolean equals(Object other) {
        CollectionUtil.requireAllNonNull(protein, carbs, fat);

        if (other == this) {
            return true;
        }

        if (!(other instanceof Food)) {
            return false;
        }

        Food otherFood = (Food) other;

        return otherFood != null
            && this.getName().equals(otherFood.getName())
            && this.getProtein().equals(otherFood.getProtein())
            && this.getCarbs().equals(otherFood.getCarbs())
            && this.getFat().equals(otherFood.getFat());
    }

    // Displays
    // name + PCF details + total calories
    @Override
    public String toString() {
        return "Food:" + this.getName() + "\n"
            + "protein: " + protein.getAmount() + "\n"
            + "carbs: " + carbs.getAmount() + "\n"
            + "fat: " + fat.getAmount() + "\n";
    }

}