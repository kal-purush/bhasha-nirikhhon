package controller;

import model.utils.Utils;
import view.LeagueLeadersForm;

import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import java.util.List;
import java.util.Objects;

public class LeagueLeadersFormController {
    private static LeagueLeadersForm leagueLeadersForm;
    private static LeagueLeadersFormController leagueLeadersFormController;

    public LeagueLeadersFormController() {
        control();
    }

    public static LeagueLeadersFormController getInstance() {
        if (Objects.isNull(leagueLeadersFormController)) {
            leagueLeadersForm = new LeagueLeadersForm();
            leagueLeadersFormController = new LeagueLeadersFormController();
        }
        init();
        return leagueLeadersFormController;
    }
    private static void init() {
        leagueLeadersForm.getFrame().setVisible(true);
        fillComboBoxFilter();
        fillComboBoxOrder();
        fillTablePlayers();
    }
    public void control() {
        leagueLeadersForm.getButtonBack().addActionListener(e -> onButtonBack());
        leagueLeadersForm.getTablePlayers().getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent e) {
                if (!e.getValueIsAdjusting()) {
                    onPlayerRowSelected();
                }
            }
        });
        leagueLeadersForm.getComboBoxFilter().addActionListener(e -> onComboBoxFilter());
        leagueLeadersForm.getComboBoxOrder().addActionListener(e -> onComboBoxFilter());
    }

    private void onComboBoxFilter() {
        String filter = (String) leagueLeadersForm.getComboBoxFilter().getSelectedItem();
        String order = (String) leagueLeadersForm.getComboBoxOrder().getSelectedItem();
        leagueLeadersForm.getTablePlayers().setModel(Utils.fillTablePlayers(MainController.playerList, filter, order));
//        Utils.addDescriptionRowToTable(leagueLeadersForm.getTablePlayers(), List.of("ID", "NAME", "LASTNAME", "TEAM", "MINUTES", "POINTS", "REBOUNDS", "ASSISTS", "STEALS", "BLOCKS", "FG MADE", "FG ATTEMPTED", "FG%", "3P MADE", "3P ATTEMPTED", "3P%", "FT MADE", "FT ATTEMPTED", "FT%", "OFF REBOUNDS", "DEF REBOUNDS", "TURNOVERS", "FOULS", "PLUS MINUS", "ID"));
        Utils.addRowNumbersToTable(leagueLeadersForm.getTablePlayers(), "#");
    }

    private void onPlayerRowSelected() {
        // Your code to handle the selected row
        int selectedRow = leagueLeadersForm.getTablePlayers().getSelectedRow();
        if (selectedRow != -1) {
            // Get the data from the selected row, e.g., using getValueAt
            Integer playerId = (Integer) leagueLeadersForm.getTablePlayers().getValueAt(selectedRow, -1);

            // Call your function with the selected data
            Utils.PARAMS.put("CURRENT_PLAYER", MainController.playerList.get(playerId));
        }
    }

    private void onButtonBack() {
        leagueLeadersForm.getFrame().dispose();
        MainFormController.getInstance();
    }
    private static void fillComboBoxFilter() {
        List<Object> stats = List.of("NAME", "LASTNAME", "MINUTES", "POINTS", "REBOUNDS", "ASSISTS", "STEALS", "BLOCKS", "FG MADE", "FG ATTEMPTED", "FG%", "3P MADE", "3P ATTEMPTED", "3P%", "FT MADE", "FT ATTEMPTED", "FT%", "OFF REBOUNDS", "DEF REBOUNDS", "TURNOVERS", "FOULS", "PLUS MINUS", "ID");
        for (Object stat : stats) {
            leagueLeadersForm.getComboBoxFilter().addItem(stat);
        }
    }

    private static void fillComboBoxOrder() {
        List<Object> stats = List.of("ASCENDING", "DESCENDING");
        for (Object stat : stats) {
            leagueLeadersForm.getComboBoxOrder().addItem(stat);
        }
    }
    private static void fillTablePlayers() {
        leagueLeadersForm.getTablePlayers().setModel(Utils.fillTablePlayers(MainController.playerList, "Points", "DESCENDING"));
//        Utils.addDescriptionRowToTable(leagueLeadersForm.getTablePlayers(), List.of("#", "NAME", "LASTNAME", "TEAM", "MINUTES", "POINTS", "REBOUNDS", "ASSISTS", "STEALS", "BLOCKS", "FG MADE", "FG ATTEMPTED", "FG%", "3P MADE", "3P ATTEMPTED", "3P%", "FT MADE", "FT ATTEMPTED", "FT%", "OFF REBOUNDS", "DEF REBOUNDS", "TURNOVERS", "FOULS", "PLUS MINUS", "ID"));
        Utils.addRowNumbersToTable(leagueLeadersForm.getTablePlayers(), "#");
        leagueLeadersForm.getComboBoxFilter().setSelectedItem("POINTS");
        leagueLeadersForm.getComboBoxOrder().setSelectedItem("DESCENDING");
    }
}