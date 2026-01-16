package seedu.address.ui;

import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.util.Pair;

/**
 * An UI component that displays information of a {@code Person}.
 */
public class TagCard extends UiPart<Region> {

    private static final String FXML = "TagListCard.fxml";

    /**
     * Note: Certain keywords such as "location" and "resources" are reserved keywords in JavaFX.
     * As a consequence, UI elements' variable names cannot be set to such keywords
     * or an exception will be thrown by JavaFX during runtime.
     *
     * @see <a href="https://github.com/se-edu/addressbook-level4/issues/336">The issue on AddressBook level 4</a>
     */

    public final Pair<String, Integer> tagPair;

    @FXML
    private HBox cardPane;
    @FXML
    private Label name;
    @FXML
    private Label id;
    @FXML
    private Label tagged;

    /**
     * Creates a {@code TagCode} with the given {@code Tag} and index to display.
     */
    public TagCard(Pair<String, Integer> tagPair) {
        super(FXML);
        this.tagPair = tagPair;
        name.setText(tagPair.getKey());
        tagged.setText(Integer.toString(tagPair.getValue()));
    }
}