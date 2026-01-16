package seedu.address.model.food;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.testutil.Assert.assertThrows;

import org.junit.jupiter.api.Test;

public class FoodTest {
    public static final String VALID_FOOD_NAME = "test food";
    public static final String VALID_FOOD_NAME_2 = "test food 2";
    public static final String INVALID_FOOD_NAME = "";

    public static final Protein PROTEIN = new Protein(2);
    public static final Protein PROTEIN_1 = new Protein(3);
    public static final Carbohydrate CARBOHYDRATE = new Carbohydrate(3);
    public static final Carbohydrate CARBOHYDRATE_1 = new Carbohydrate(4);
    public static final Fat FAT = new Fat(4);
    public static final Fat FAT_1 = new Fat(5);

    public static final Food COMPARED_FOOD = new Food(VALID_FOOD_NAME, PROTEIN, CARBOHYDRATE, FAT);
    public static final Food SAME_AS_COMPARED_FOOD = new Food(VALID_FOOD_NAME, PROTEIN, CARBOHYDRATE, FAT);
    public static final Food FOOD_W_DIFFERENT_NAME = new Food(VALID_FOOD_NAME_2, PROTEIN, CARBOHYDRATE, FAT);
    public static final Food FOOD_W_DIFFERENT_PROTEIN = new Food(VALID_FOOD_NAME, PROTEIN_1, CARBOHYDRATE, FAT);
    public static final Food FOOD_W_DIFFERENT_CARBS = new Food(VALID_FOOD_NAME, PROTEIN, CARBOHYDRATE_1, FAT);
    public static final Food FOOD_W_DIFFERENT_FAT = new Food(VALID_FOOD_NAME, PROTEIN, CARBOHYDRATE, FAT_1);

    @Test
    public void constructor_nullProtein_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () ->
            new Food(VALID_FOOD_NAME, null, CARBOHYDRATE, FAT));
    }

    @Test
    public void constructor_nullCarbohydrate_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () ->
            new Food(VALID_FOOD_NAME, PROTEIN, null, FAT));
    }

    @Test
    public void constructor_nullFat_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () ->
            new Food(VALID_FOOD_NAME, PROTEIN, CARBOHYDRATE, null));
    }

    @Test
    public void constructor_invalidName_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
            new Food(INVALID_FOOD_NAME, PROTEIN, CARBOHYDRATE, FAT));
    }

    @Test
    public void equals() {
        // identical -> returns true
        assertEquals(COMPARED_FOOD, COMPARED_FOOD);

        // different object all field are the same -> returns true
        assertEquals(COMPARED_FOOD, SAME_AS_COMPARED_FOOD);

        // different protein -> returns false
        assertFalse(COMPARED_FOOD.equals(FOOD_W_DIFFERENT_PROTEIN));

        // different carbohydrate -> returns false
        assertFalse(COMPARED_FOOD.equals(FOOD_W_DIFFERENT_CARBS));

        // different fat -> returns false
        assertFalse(COMPARED_FOOD.equals(FOOD_W_DIFFERENT_FAT));

        // different type -> returns false
        assertFalse(COMPARED_FOOD.equals(PROTEIN));
    }



}