package seedu.address.ui;

import java.util.Comparator;

import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.util.Pair;
import seedu.address.model.person.Person;

/**
 * An UI component that displays information of a {@code Person}.
 */
public class CourseCard extends UiPart<Region> {

    private static final String FXML = "CourseListCard.fxml";

    /**
     * Note: Certain keywords such as "location" and "resources" are reserved keywords in JavaFX.
     * As a consequence, UI elements' variable names cannot be set to such keywords
     * or an exception will be thrown by JavaFX during runtime.
     *
     * @see <a href="https://github.com/se-edu/addressbook-level4/issues/336">The issue on AddressBook level 4</a>
     */

    public final Pair<String, Integer> coursePair;

    @FXML
    private HBox cardPane;
    @FXML
    private Label name;
    @FXML
    private Label id;
    @FXML
    private Label taking;

    /**
     * Creates a {@code CourseCode} with the given {@code Course} and index to display.
     */
    public CourseCard(Pair<String, Integer> coursePair, int displayedIndex) {
        super(FXML);
        this.coursePair = coursePair;
        id.setText(displayedIndex + ". ");
        name.setText(coursePair.getKey());
        taking.setText(Integer.toString(coursePair.getValue()));
    }
}