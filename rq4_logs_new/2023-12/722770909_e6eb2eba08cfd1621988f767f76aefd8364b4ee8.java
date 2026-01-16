package controller;

import model.UtilsDatabase;
import org.postgresql.core.Utils;
import view.StandingsForm;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class StandingsFormController {
    private static StandingsForm standingsForm;
    private static StandingsFormController standingsFormController;


    public StandingsFormController() {
        control();
    }

    public static StandingsFormController getInstance() {
        if (Objects.isNull(standingsFormController)) {
            standingsForm = new StandingsForm();
            standingsFormController = new StandingsFormController();
        }
        init();
        return standingsFormController;
    }

    public void control() {
    }

    private static void init() {
        standingsForm.getFrame().setVisible(true);
        initiallyFillTable();
    }

    private static void initiallyFillTable() {
        try {
            UtilsDatabase.fillTableFromResultSet(standingsForm.getTableStandings(), UtilsDatabase.runSqlFunction("get_standings", List.of()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}