package com.controller;

import com.character.Player;
import com.gameEngine.GameEngine;
import com.item.Item;
import com.location.Locations;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Label;
import javafx.scene.control.Slider;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.input.KeyCode;
import javafx.scene.shape.Circle;
import javafx.scene.text.Font;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.ResourceBundle;

public class GameSceneControllerNew implements Initializable {

    @FXML
    private TextArea storyTextArea;

    @FXML
    private TextArea inventory;

    @FXML
    private TextField inputTextField;

    @FXML
    private Label currentLocation;

    @FXML
    private Circle currentLocationCircle;

    @FXML
    private Slider fontSlider;


    //    private final GameEngine gameEngine = GameEngine.getInstance();
    private final GameEngine gameEngine = GameEngine.GAME_ENGINE;


    private final HashMap<String, double[]> mapCoordinates = new HashMap<>();

    public GameSceneControllerNew() {
        mapCoordinates.put("Ship", new double[] {151,689});
        mapCoordinates.put("Repair Workshop", new double[] {290,538});
        mapCoordinates.put("Landing Dock", new double[] {182,482});
        mapCoordinates.put("Escape Shuttle", new double[] {62,395});
        mapCoordinates.put("Central Hub", new double[] {198,278});
        mapCoordinates.put("Bar", new double[] {262,111});
        mapCoordinates.put("Medical Bay", new double[] {80,118});
    }

    private TextField getInputTextField() {
        return inputTextField;
    }


    /*
     * handles reading of user input from the Text Input Field in the Game Scene.
     * displays the read text in the uneditable TextArea field
     */
    public void handleTextFieldInput(ActionEvent event) {
        getInputTextField().setOnKeyPressed(keyEvent -> {
            if (keyEvent.getCode().equals(KeyCode.ENTER)) {
                storyTextArea.setText(" > " + inputTextFieldString() + "\n");
                String append = String.valueOf(gameEngine.runGameLoop(inputTextFieldString()));
                storyTextArea.appendText(append);
                setTextColor();

                getPlayerInventory();
                getPlayerCurrentLocation();
                getInputTextField().clear();
            }
        });
    }

    private void setTextColor() {
        if(storyTextArea.getText().contains("fight")){
            storyTextArea.setStyle("-fx-text-fill : red; -fx-control-inner-background: black;");
        }else if(storyTextArea.getText().contains("talk")){
            storyTextArea.setStyle("-fx-text-fill : yellow; -fx-control-inner-background: black;");
        }else if(storyTextArea.getText().contains("go")){
            storyTextArea.setStyle("-fx-text-fill : blue; -fx-control-inner-background: black;");
        }else{
            storyTextArea.setStyle("-fx-text-fill : green; -fx-control-inner-background: black;");
        }
    }

    private void getPlayerCurrentLocation() {
        String currentLocationName = Player.PLAYER.getCurrentLocation().getName();
        currentLocation.setText(String.valueOf("Current Room: " + currentLocationName));
//        System.out.println(mapCoordinates.get(gameEngine.currentLocation)[0]);
        currentLocationCircle.setLayoutX(mapCoordinates.get(currentLocationName)[0]);
        currentLocationCircle.setLayoutY(mapCoordinates.get(currentLocationName)[1]);
    }

    private void getPlayerInventory() {
        StringBuilder playerInventory = new StringBuilder();
        for (Item item : Player.PLAYER.getInventory()) {
            playerInventory.append(item.getName()).append("\n");
            inventory.setText(String.valueOf(playerInventory));
        }
    }
    /*
     * initialized at start of game.
     * reads main story txt file and sends add it to the textarea
     */
    private void introStoryToTextarea() throws IOException {
        File file = new File("cfg/IntoStory.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String str;
        while ((str = br.readLine()) != null) {
            storyTextArea.appendText(str + '\n');
        }
        setFontSlider();
    }

    private void setFontSlider() {
        if(fontSlider != null){
            fontSlider.valueProperty().addListener(new ChangeListener<Number>() {
                @Override
                public void changed(ObservableValue<? extends Number> observableValue, Number number, Number t1) {
                    storyTextArea.setFont(Font.font(t1.doubleValue()));
                }
            });
        }
    }


    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        try {
            setFontSlider();
            introStoryToTextarea();
            Locations.initWithJsonFile("cfg/sampleLocations.json");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String inputTextFieldString() {
        return getInputTextField().getText();
    }

    private void appendInputToStoryTextarea(String strToDisplay) {
        storyTextArea.appendText(strToDisplay + '\n');
    }




}