package jimmy.mcgymmy.ui;

import static java.util.Objects.requireNonNull;

import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.layout.AnchorPane;

public class SummaryDisplay extends UiPart<AnchorPane> {
    private static final String FXML = "SummaryDisplay.fxml";
    private static final String TOTAL_CALORIES_TEXT = "Total Calories %d";

    @FXML
    private Label totalCalories;

    /**
     * Creates a summary display.
     * Contains summary of the items shown.
     */
    public SummaryDisplay() {
        super(FXML);
        setTotalCalories(200);
    }

    /**
     * Set the label to show the correct total calories.
     * @param totalCalories total calories in current items.
     */
    public void setTotalCalories(int totalCalories) {
        requireNonNull(totalCalories);
        this.totalCalories.setText(String.format(TOTAL_CALORIES_TEXT, totalCalories));
    }
}