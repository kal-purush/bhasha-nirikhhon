package seedu.storemando.ui;

import static java.time.temporal.ChronoUnit.DAYS;

import java.net.URL;
import java.time.LocalDate;
import java.util.ResourceBundle;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.Region;
import seedu.storemando.model.item.Item;

/**
 * Table displaying all items expiring in a week
 */
public class TablePanel extends UiPart<Region> implements Initializable {
    private static final String FXML = "TablePanel.fxml";
    private ObservableList<Item> itemList;

    @FXML
    private TableView<ExpiringItem> tableView;

    @FXML
    private TableColumn<ExpiringItem, String> daysBeforeExpiryCol;

    @FXML
    private TableColumn<ExpiringItem, String> numberOfItems;

    /**
     * Creates a {@code TablePanel} with the given {@code ObservableList}.
     */
    public TablePanel(ObservableList<Item> itemList) {
        super(FXML);
        this.itemList = itemList;
        tableView.setItems(getExpiringItems(itemList));
    }

    /**
     * Passing the data into the table.
     * @param itemList The list of data required by the table.
     * @return A list of items expiring in a week.
     */
    private ObservableList<ExpiringItem> getExpiringItems(ObservableList<Item> itemList) {
        ObservableList<ExpiringItem> tableRows = FXCollections.observableArrayList();
        for (int i = 1; i <= 7; i++) {
            tableRows.add(new ExpiringItem(i, itemList));
        }
        return tableRows;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        daysBeforeExpiryCol.setCellValueFactory(new PropertyValueFactory<ExpiringItem, String>("daysBeforeExpiry"));
        numberOfItems.setCellValueFactory(new PropertyValueFactory<ExpiringItem, String>("numberOfItems"));
        tableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
    }

    /**
     * Item that are expiring in a week.
     */
    public class ExpiringItem {
        private String daysBeforeExpiry;
        private String numberOfItems;

        public ExpiringItem(int daysBeforeExpiry, ObservableList<Item> itemList) {
            this.daysBeforeExpiry = String.valueOf(daysBeforeExpiry) + " days from expiring";
            this.numberOfItems = Long.toString(getExpiringItemWithinDays(daysBeforeExpiry, itemList)) + " items";
        }

        /**
         * Get the number of items that is expiring in the specified number of days.
         * @param day The specified number of days.
         * @param itemList The list of items in StoreMando.
         * @return The number of items expiring in the specified number of days.
         */
        private long getExpiringItemWithinDays(int day, ObservableList<Item> itemList) {
            LocalDate today = LocalDate.now();
            return itemList.stream().filter(item -> {
                if (item.getExpiryDate().expiryDate == null) {
                    return false;
                }
                LocalDate itemExpiryDate = item.getExpiryDate().expiryDate;
                Long daysDifference = DAYS.between(today, itemExpiryDate);
                return daysDifference == day;
            }).map(item -> Integer.parseInt(item.getQuantity().toString()))
                .reduce(0, (initialCount, quantity) -> initialCount + quantity);
        }

        public String getDaysBeforeExpiry() {
            return daysBeforeExpiry;
        }

        public String getNumberOfItems() {
            return numberOfItems;
        }
    }
}