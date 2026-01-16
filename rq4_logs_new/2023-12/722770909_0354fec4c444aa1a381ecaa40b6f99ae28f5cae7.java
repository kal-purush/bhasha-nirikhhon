package controller;

import model.UtilsDatabase;
import view.GamesForm;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;
import java.util.Objects;

public class GamesFormController {
    private static GamesForm gamesForm;
    private static GamesFormController gamesFormController;

    public GamesFormController() {
        control();
    }

    public static GamesFormController getInstance() {
        if (Objects.isNull(gamesFormController)) {
            gamesForm = new GamesForm();
            gamesFormController = new GamesFormController();
        }
        init();
        return gamesFormController;
    }

    public void control() {
        gamesForm.getComboBoxMonth().addActionListener(e -> onComboBoxMonth());
        gamesForm.getComboBoxYear().addActionListener(e -> onComboBoxYear());
        gamesForm.getComboBoxDay().addActionListener(e -> onComboBoxDay());
        gamesForm.getButtonSearch().addActionListener(e -> {
            try {
                onButtonSearch();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    private static void init() {
        gamesForm.getFrame().setVisible(true);
        fillYearComboBox();
        fillMonthComboBox();
        fillDayComboBox();
    }

    private void onComboBoxMonth() {
        gamesForm.getButtonSearch().setEnabled(!Objects.isNull(gamesForm.getComboBoxMonth().getSelectedItem()) && !Objects.isNull(gamesForm.getComboBoxYear().getSelectedItem()) && !Objects.isNull(gamesForm.getComboBoxDay().getSelectedItem()));
        gamesForm.getComboBoxDay().removeAllItems();
        int daysInMonth = LocalDate.of((int) gamesForm.getComboBoxYear().getSelectedItem(), (Month) gamesForm.getComboBoxMonth().getSelectedItem(), 1).lengthOfMonth();
        for (int day = 1; day <= daysInMonth; day++) {
            gamesForm.getComboBoxDay().addItem(day);
        }
    }
    private void onButtonSearch() throws SQLException {
        UtilsDatabase.populateJListFromResultSet(gamesForm.getListGames(), UtilsDatabase.runSqlFunction("get_games_by_date", List.of(gamesForm.getComboBoxYear().getSelectedItem() + "-"  + gamesForm.getComboBoxMonth().getSelectedItem() + "-" + gamesForm.getComboBoxDay().getSelectedItem())));
    }

    private void onComboBoxDay() {
        gamesForm.getButtonSearch().setEnabled(!Objects.isNull(gamesForm.getComboBoxMonth().getSelectedItem()) && !Objects.isNull(gamesForm.getComboBoxYear().getSelectedItem()) && !Objects.isNull(gamesForm.getComboBoxDay().getSelectedItem()));
    }

    private void onComboBoxYear() {
        gamesForm.getButtonSearch().setEnabled(!Objects.isNull(gamesForm.getComboBoxMonth().getSelectedItem()) && !Objects.isNull(gamesForm.getComboBoxYear().getSelectedItem()) && !Objects.isNull(gamesForm.getComboBoxDay().getSelectedItem()));
    }

    private static void fillYearComboBox() {
        int currentYear = LocalDate.now().getYear();
        gamesForm.getComboBoxYear().addItem(currentYear);
        gamesForm.getComboBoxYear().addItem(currentYear + 1);
    }

    private static void fillMonthComboBox() {
        for (Month month : Month.values()) {
            gamesForm.getComboBoxMonth().addItem(month);
        }
    }

    private static void fillDayComboBox() {
        int daysInMonth = LocalDate.now().lengthOfMonth();
        for (int day = 1; day <= daysInMonth; day++) {
            gamesForm.getComboBoxDay().addItem(day);
        }
    }
}