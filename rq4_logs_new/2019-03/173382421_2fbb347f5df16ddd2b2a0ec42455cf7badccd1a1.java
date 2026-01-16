package controller;

import data.interfaces.IGame;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Control;
import javafx.scene.control.Label;
import javafx.scene.layout.*;
import javafx.scene.text.TextAlignment;
import javafx.stage.Stage;

import java.util.ArrayList;
import java.util.List;

/**
 * Every Game will be started in a separate Stage. This GameController create a new Game Stage and manage it.
 */
public class GameController {

    private IGame Game;

    private Stage stage;
    private Scene SelectionScene;
    private List<Button> buttons;

    // region Constructors

    public GameController(IGame game) {
        Game = game;
    }

    public GameController() {
    }

    // endregion

    public void setGame(IGame game) {
        this.Game = game;
    }

    /**
     * Open the given Game in a separate Stage
     */
    public void ShowGame() {
        stage = new Stage();
        stage.setTitle(Game.getName());
        if (SelectionScene == null) {
            SelectionScene = createSelectionScene();
        }
        stage.setScene(SelectionScene);
        stage.show();
    }

    /**
     * create a new Scene with the first Pane on that an Button for each Question exist.
     *
     * @return Scene with Selection Pane
     */
    private Scene createSelectionScene() {
        GridPane buttonGrid = new GridPane();
//        buttonGrid.setPadding(new Insets(50));
        for (int i = 0; i < Game.getCategories().size(); i++) {
            ColumnConstraints cc = new ColumnConstraints();
            cc.setPercentWidth(100.0 / Game.getCategories().size());
            cc.setHgrow(Priority.NEVER);
            buttonGrid.getColumnConstraints().add(cc);
        }

        for (int i = 0; i < Game.getQuestionAmount() + 1; i++) {
            RowConstraints rc = new RowConstraints();
            rc.setVgrow(Priority.ALWAYS);
            buttonGrid.getRowConstraints().add(rc);
        }

        EventHandler<ActionEvent> eh = event -> {
            if (event.getSource() instanceof Button) {
                String[] parts = ((Button) event.getSource()).getId().split(",", 2);
                // TODO show Question
                System.out.println("Event Handler works");
            }
        };

        buttons = new ArrayList<>();
        for (int i = 0; i < Game.getCategories().size(); i++) {
            Label name = new Label(Game.getCategories().get(i).getName());
            name.setTextAlignment(TextAlignment.CENTER);
            name.setWrapText(true);
            HBox box = new HBox(name);
            box.setAlignment(Pos.CENTER);
            buttonGrid.add(box, i, 0);
            for (int j = 0; j < 5; j++) {
                Button b = new Button(((j + 1) * 20) + "");
                b.setId(b.getText() + "," + i);
                b.setOnAction(eh);
                setAnchors(b, 20);
                buttons.add(b);
                buttonGrid.add(new AnchorPane(b), i, j + 1);
            }
        }

        // TODO Teams
        return new Scene(buttonGrid);
    }

    /**
     * set Space between Control edge of AnchorPane
     *
     * @param control Control in an AnchorPane
     * @param margin Space to the edge of the AnchorPane
     */
    private void setAnchors(Control control, double margin) {
        AnchorPane.setBottomAnchor(control, margin);
        AnchorPane.setLeftAnchor(control, margin);
        AnchorPane.setRightAnchor(control, margin);
        AnchorPane.setTopAnchor(control, margin);
    }
}