package view;

import interface_adapter.recipe_details.RecipeDetailsViewModel;
import interface_adapter.signup.SignupViewModel;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

public class RecipeDetailsView extends JPanel implements ActionListener, PropertyChangeListener {

    private final RecipeDetailsViewModel recipeDetailsViewModel;

    public RecipeDetailsView(RecipeDetailsViewModel recipeDetailsViewModel) {
        this.recipeDetailsViewModel = recipeDetailsViewModel;
        recipeDetailsViewModel.addPropertyChangeListener(this);

        final JLabel title = new JLabel(RecipeDetailsViewModel.TITLE_LABEL);
        title.setAlignmentX(Component.CENTER_ALIGNMENT);
    }

    @Override
    public void actionPerformed(ActionEvent e) {

    }

    @Override
    public void propertyChange(PropertyChangeEvent evt) {

    }
}