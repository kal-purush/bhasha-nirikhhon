package seedu.storemando.ui;

import static java.time.temporal.ChronoUnit.DAYS;

import java.net.URL;
import java.time.LocalDate;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.Region;
import seedu.storemando.commons.core.LogsCenter;
import seedu.storemando.model.item.Item;

/**
 * Table displaying all expired items.
 */
public class ExpiredTablePanel extends UiPart<Region> implements Initializable {
    private static final int EXPIRED_FOR_MORE_THAN_EIGHT_DAYS = -8;
    private static final String FXML = "TablePanel.fxml";
    private final Logger logger = LogsCenter.getLogger(getClass());
    private ObservableList<Item> itemList;

    @FXML
    private TableView<ExpiredItems> tableView;

    @FXML
    private TableColumn<ExpiredItems, String> numberOfDays;

    @FXML
    private TableColumn<ExpiredItems, String> numberOfItems;

    /**
     * Creates a {@code TablePanel} with the given {@code ObservableList}.
     */
    public ExpiredTablePanel(ObservableList<Item> itemList) {
        super(FXML);
        this.itemList = itemList;
        tableView.setItems(getExpiredItemss(itemList));
    }

    /**
     * Passing the data into the table.
     * @param itemList The list of data required by the table.
     * @return A list of items expiring in a week.
     */
    private ObservableList<ExpiredItems> getExpiredItemss(ObservableList<Item> itemList) {
        ObservableList<ExpiredItems> tableRows = FXCollections.observableArrayList();
        for (int i = -1; i >= -7; i--) {
            tableRows.add(new ExpiredItems(i, itemList));
        }
        tableRows.add(new ExpiredItems(EXPIRED_FOR_MORE_THAN_EIGHT_DAYS, itemList));
        return tableRows;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        numberOfDays.setCellValueFactory(new PropertyValueFactory<ExpiredItems, String>("daysAfterExpiry"));
        numberOfItems.setCellValueFactory(new PropertyValueFactory<ExpiredItems, String>("numberOfItems"));
        tableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        try {
            tableView.setSelectionModel(null);
        } catch (NullPointerException ex) {
            logger.info("User tried to click on the expiring item's table");
        }
    }

    /**
     * Item that are expired.
     */
    public class ExpiredItems {
        private String daysAfterExpiry;
        private String numberOfItems;

        /**
         * Expired Item constructor.
         * @param daysAfterExpiry   The number of days before the item expire.
         * @param itemList   The list of items in StoreMando.
         */
        public ExpiredItems(int daysAfterExpiry, ObservableList<Item> itemList) {
            long numOfItems;
            this.daysAfterExpiry = convertDaysInIntToString(daysAfterExpiry);
            if (daysAfterExpiry <= EXPIRED_FOR_MORE_THAN_EIGHT_DAYS) {
                numOfItems = getExpiredItemMoreThan7days(itemList);
            } else {
                numOfItems = getExpiredItemLessThan7Days(daysAfterExpiry, itemList);
            }
            this.numberOfItems = convertNumOfItemInIntToString(numOfItems);
        }

        /**
         * Convert days that is a integer type to a string.
         * @param daysAfterExpiry The days in integer that will be converted to String.
         * @return The string representation of days.
         */
        private String convertDaysInIntToString(int daysAfterExpiry) {
            if (daysAfterExpiry <= EXPIRED_FOR_MORE_THAN_EIGHT_DAYS) {
                return "More than 7 days";
            } else if (daysAfterExpiry == -1) {
                return String.valueOf(Math.abs(daysAfterExpiry)) + " day";
            } else {
                return String.valueOf(Math.abs(daysAfterExpiry)) + " days";
            }
        }

        /**
         * Convert number of items that is a integer to a string
         * @param numberOfItems The number of items in integer that will be converted to string
         * @return The string representation of number of items.
         */
        private String convertNumOfItemInIntToString(long numberOfItems) {
            if (numberOfItems <= 1) {
                return Long.toString(numberOfItems) + " item";
            } else {
                return Long.toString(numberOfItems) + " items";
            }
        }

        /**
         * Get the number of items that has expired for specified number of days.
         * @param day The specified number of days.
         * @param itemList The list of items in StoreMando.
         * @return The number of items expiring in the specified number of days.
         */
        private long getExpiredItemLessThan7Days(int day, ObservableList<Item> itemList) {
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

        /**
         * Get the number of items that has expired for more than 7 days.
         * @param itemList The list of items in StoreMando.
         * @return The number of items expired for more than 7 days.
         */
        private long getExpiredItemMoreThan7days(ObservableList<Item> itemList) {
            LocalDate today = LocalDate.now();
            return itemList.stream().filter(item -> {
                if (item.getExpiryDate().expiryDate == null) {
                    return false;
                }
                LocalDate itemExpiryDate = item.getExpiryDate().expiryDate;
                Long daysDifference = DAYS.between(today, itemExpiryDate);
                return daysDifference <= EXPIRED_FOR_MORE_THAN_EIGHT_DAYS;
            }).map(item -> Integer.parseInt(item.getQuantity().toString()))
                .reduce(0, (initialCount, quantity) -> initialCount + quantity);
        }

        public String getDaysAfterExpiry() {
            return daysAfterExpiry;
        }

        public String getNumberOfItems() {
            return numberOfItems;
        }
    }
}