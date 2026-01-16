package interface_adapter.initial;

import interface_adapter.ViewModel;

/**
 * The View Model for the Initial View.
 */
public class InitialViewModel extends ViewModel<InitialState> {

    public static final String TITLE_LABEL = "Your Fridge";

    public static final String ADD_INGREDIENT_BUTTON_LABEL = "Add Ingredient";
    public static final String GENERATE_RECIPE_BUTTON_LABEL = "Generate Recipe";

    public InitialViewModel() {
        super("initial");
        setState(new InitialState());
    }

}