package interface_adapter.recipemanagement;

import entity.Recipe;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

/**
 * ViewModel for the RecipeInfoView.
 * Manages the state of the RecipeInfoView and notifies it of updates.
 */
public class RecipeInfoViewModel {
    private final PropertyChangeSupport support;
    private Recipe currentRecipe;

    public RecipeInfoViewModel() {
        this.support = new PropertyChangeSupport(this);
    }

    /**
     * Sets the current recipe and notifies listeners of the change.
     * @param recipe the new recipe to display
     */
    public void setCurrentRecipe(Recipe recipe) {
        Recipe oldRecipe = this.currentRecipe;
        this.currentRecipe = recipe;
        support.firePropertyChange("currentRecipe", oldRecipe, recipe);
    }

    /**
     * Gets the current recipe.
     * @return the current recipe
     */
    public Recipe getCurrentRecipe() {
        return currentRecipe;
    }

    /**
     * Adds a PropertyChangeListener to listen for updates.
     * @param listener the listener to add
     */
    public void addPropertyChangeListener(PropertyChangeListener listener) {
        support.addPropertyChangeListener(listener);
    }

    /**
     * Removes a PropertyChangeListener.
     * @param listener the listener to remove
     */
    public void removePropertyChangeListener(PropertyChangeListener listener) {
        support.removePropertyChangeListener(listener);
    }

    public String getViewName() {
        return "RecipeInfoViewModel";
    }
}