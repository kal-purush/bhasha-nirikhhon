package controller;

import model.Game;
import model.utils.Utils;
import model.utils.UtilsDatabase;
import view.BoxScoreForm;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class BoxScoreFormController {
    private static BoxScoreForm boxScoreForm;
    private static BoxScoreFormController boxScoreFormController;

    public BoxScoreFormController() {
        control();
    }

    public static BoxScoreFormController getInstance() {
        if (Objects.isNull(boxScoreFormController)) {
            boxScoreForm = new BoxScoreForm();
            boxScoreFormController = new BoxScoreFormController();
        }
        init();
        return boxScoreFormController;
    }

    public void control() {
        boxScoreForm.getButtonHomeTeam().addActionListener(e -> onButtonHomeTeam());
        boxScoreForm.getButtonAwayTeam().addActionListener(e -> onButtonAwayTeam());
        boxScoreForm.getFrame().addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent windowEvent) {
                GamesFormController.getInstance();
            }
        });
    }

    private static void init() {
        boxScoreForm.getFrame().setVisible(true);
        try {
            setGame();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void setGame() throws SQLException {
        Game game = (Game) Utils.PARAMS.get("CURRENT_GAME");
        boxScoreForm.getLabelDescription().setText(game.toString());
        boxScoreForm.getButtonHomeTeam().setText(game.getHomeTeam());
        boxScoreForm.getButtonAwayTeam().setText(game.getAwayTeam());
        boxScoreForm.getButtonHomeTeam().setEnabled(false);
        UtilsDatabase.fillTableFromResultSet( boxScoreForm.getTableBoxScore(), UtilsDatabase.runSqlFunction("get_player_box_score", List.of(game.getId(), game.getIdHomeTeam())));
    }

    private void onButtonHomeTeam() {
        boxScoreForm.getButtonAwayTeam().setEnabled(true);
        boxScoreForm.getButtonHomeTeam().setEnabled(false);
        try {
            UtilsDatabase.fillTableFromResultSet(boxScoreForm.getTableBoxScore(), UtilsDatabase.runSqlFunction("get_player_box_score", List.of(((Game) Utils.PARAMS.get("CURRENT_GAME")).getId(), ((Game) Utils.PARAMS.get("CURRENT_GAME")).getIdHomeTeam())));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    private void onButtonAwayTeam() {
        boxScoreForm.getButtonAwayTeam().setEnabled(false);
        boxScoreForm.getButtonHomeTeam().setEnabled(true);
        try {
            UtilsDatabase.fillTableFromResultSet(boxScoreForm.getTableBoxScore(), UtilsDatabase.runSqlFunction("get_player_box_score", List.of(((Game) Utils.PARAMS.get("CURRENT_GAME")).getId(), ((Game) Utils.PARAMS.get("CURRENT_GAME")).getIdAwayTeam())));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}