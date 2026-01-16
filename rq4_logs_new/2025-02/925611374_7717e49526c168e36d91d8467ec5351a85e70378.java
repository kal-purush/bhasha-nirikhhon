package com.cgvsu.GUI;

import com.cgvsu.math.AffineTransformations;
import com.cgvsu.math.TranslationModel;
import com.cgvsu.render_engine.RenderEngine;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.scene.canvas.Canvas;
import javafx.scene.control.*;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.paint.Color;
import javafx.stage.Stage;
import javafx.stage.FileChooser;
import javafx.util.Duration;

import java.nio.file.Path;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.vecmath.Matrix4f;
import javax.vecmath.Vector3f;

import com.cgvsu.model.Model;
import com.cgvsu.render_engine.Camera;

public class GuiController {

    final private float TRANSLATION = 0.9F; //шаг перемещения камеры

    private final Scene scene = new Scene();

    public ColorPicker baseModelColor = new ColorPicker();
    public CheckBox transformSave;

    public AnchorPane modelPane;
    public TextField sx;
    public TextField sy;
    public TextField sz;
    public TextField tx;
    public TextField ty;
    public TextField tz;
    public TextField rx;
    public TextField ry;
    public TextField rz;
    public Button convert;
    public TextField deleteVertex;
    public Button deleteVertexButton;
    //вращение
    private boolean isMousePressedForModel = false;
    private double lastMouseXForModel;
    private double lastMouseYForModel;
    private double rotationSensitivity = 0.5;
    private final float zoomSpeed = 0.5f;

    Alert messageWarning = new Alert(Alert.AlertType.WARNING);
    Alert messageError = new Alert(Alert.AlertType.ERROR);
    Alert messageInformation = new Alert(Alert.AlertType.INFORMATION);

    @FXML
    public AnchorPane gadgetPane;
    @FXML
    public AnchorPane cameraPane;
    public TextField eyeX;
    public TextField targetX;
    public TextField eyeY;
    public TextField targetY;
    public TextField eyeZ;
    public TextField targetZ;
    @FXML
    private MenuItem menuOpen;
    @FXML
    private MenuItem menuSave;
    @FXML
    private MenuItem menuSwitchTheme;
    @FXML
    AnchorPane anchorPane;

    @FXML
    private Canvas canvas;

    private final List<Camera> cameras = new ArrayList<>();
    private final List<Button> addedButtonsCamera = new ArrayList<>();
    private final List<Button> deletedButtonsCamera = new ArrayList<>();


    private final List<Button> addedButtonsModel = new ArrayList<>();
    private final List<CheckBox> checkBoxesTexture = new ArrayList<>();
    private final List<CheckBox> checkBoxesLighting = new ArrayList<>();
    private final List<CheckBox> checkBoxesGrid = new ArrayList<>();
    private final List<RadioButton> choiceModelRadioButtons = new ArrayList<>();
    private final List<Button> deletedButtonsModel = new ArrayList<>();

    private Timeline timeline;
    private boolean isDarkTheme = false;

    @FXML
    private void initialize() throws IOException {
        anchorPane.prefWidthProperty().addListener((ov, oldValue, newValue) -> canvas.setWidth(newValue.doubleValue()));
        anchorPane.prefHeightProperty().addListener((ov, oldValue, newValue) -> canvas.setHeight(newValue.doubleValue()));

        gadgetPane.prefWidthProperty().addListener((ov, oldValue, newValue) -> canvas.setWidth(newValue.doubleValue()));
        gadgetPane.prefHeightProperty().addListener((ov, oldValue, newValue) -> canvas.setHeight(newValue.doubleValue()));

        timeline = new Timeline();
        timeline.setCycleCount(Animation.INDEFINITE);
        createCamera();

        baseModelColor.setValue(Color.GRAY);

        KeyFrame frame = new KeyFrame(Duration.millis(50), event -> {
            double width = canvas.getWidth();
            double height = canvas.getHeight();

            canvas.getGraphicsContext2D().clearRect(0, 0, width, height);
            for (Camera c : cameras) {
                c.setAspectRatio((float) (width / height));
            }
            if (!scene.getMeshes().isEmpty()) {
                RenderEngine.render(canvas.getGraphicsContext2D(), activeCamera(), scene.getMeshes(), (int) width, (int) height);
            }
        });

        timeline.getKeyFrames().add(frame);
        timeline.play();
    }

    private void showMessage(String headText, String messageText, Alert alert) {
        alert.setHeaderText(headText);
        alert.setContentText(messageText);
        alert.showAndWait();
    }

    private Camera activeCamera() {
        for (Camera camera : cameras) {
            if (camera.isActive()) {
                return camera;
            }
        }
        showMessage("Осторожно", "Переключено на Камеру 1", messageInformation);
        return cameras.get(0);
    }

    @FXML
    public void moveCamera(KeyEvent keyEvent) {
        switch (keyEvent.getCode()) {
            case W -> activeCamera().movePosition(new Vector3f(0, 0, -TRANSLATION));
            case S -> activeCamera().movePosition(new Vector3f(0, 0, TRANSLATION));
            case A -> activeCamera().movePosition(new Vector3f(TRANSLATION, 0, 0));
            case D -> activeCamera().movePosition(new Vector3f(-TRANSLATION, 0, 0));
            case R -> activeCamera().movePosition(new Vector3f(0, TRANSLATION, 0));
            case F -> activeCamera().movePosition(new Vector3f(0, -TRANSLATION, 0));
        }
    }

    @FXML
    public void menuSaveAction(ActionEvent event) {
        if (scene.getMeshes().isEmpty()) {
            showMessage("Предупреждение", "Откройте модель для сохранения!", messageWarning);
            return;
        }
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Save Model");
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Model (*.obj)", "*.obj"));

        File file = fileChooser.showSaveDialog(getStage());
        if (file == null) {
            return;
        }
        try {
            boolean applyTransform = false;
            scene.saveModel(Path.of(file.getAbsolutePath()), applyTransform);
            showMessage("Информация", "Модель успешно сохранена!", messageInformation);
        } catch (RuntimeException ex) {
            showMessage("Ошибка", ex.getMessage(), messageError);
        }
    }

    private Stage getStage() {
        return (Stage) anchorPane.getScene().getWindow();
    }

    @FXML
    public void menuSwitchThemeAction(ActionEvent event) {
        javafx.scene.Scene javafxScene = anchorPane.getScene();
        if (javafxScene == null) return;

        javafxScene.getStylesheets().clear();
        isDarkTheme = !isDarkTheme;

        if (isDarkTheme) {
            javafxScene.getStylesheets().add(
                    Objects.requireNonNull(
                            getClass().getResource("/com/cgvsu/fxml/dark-theme.css")
                    ).toExternalForm()
            );
        } else {
            javafxScene.getStylesheets().add(
                    Objects.requireNonNull(
                            getClass().getResource("/com/cgvsu/fxml/light-theme.css")
                    ).toExternalForm()
            );
        }
    }

    @FXML
    public void menuOpenAction(ActionEvent event) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Open Model");
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Model (*.obj)", "*.obj"));
        File file = fileChooser.showOpenDialog(getStage());
        if (file == null) {
            return;
        }
        try {
            Model mesh = scene.openModel(Path.of(file.getAbsolutePath()));
            scene.addModel(mesh);
            addModelButtons();
        } catch (IOException ex) {
            showMessage("Ошибка", "Не удалось прочитать файл!", messageError);
        }
    }


    public void addCameraButtons() {
        Button addButton = new Button("Камера " + (addedButtonsCamera.size() + 1));
        addButton.setLayoutY((addedButtonsCamera.size() > 0) ?
                addedButtonsCamera.get(addedButtonsCamera.size() - 1).getLayoutY() + 40 :
                185);
        addButton.setLayoutX(33);
        addButton.setOnAction(event -> showCamera(addButton.getText()));
        addedButtonsCamera.add(addButton);
        Button deleteButton = new Button("Удалить");
        deleteButton.setLayoutY(addedButtonsCamera.get(addedButtonsCamera.size() - 1).getLayoutY());
        deleteButton.setLayoutX(addedButtonsCamera.get(addedButtonsCamera.size() - 1).getLayoutX() + 85);
        deleteButton.setOnAction(event -> deleteCamera(addButton.getText()));
        deletedButtonsCamera.add(deleteButton);

        cameraPane.getChildren().add(addButton);
        cameraPane.getChildren().add(deleteButton);
    }

    public void addModelButtons() {
        Button addButton = new Button("Модель " + (addedButtonsModel.size() + 1));
        addButton.setLayoutY((addedButtonsModel.size() > 0) ?
                addedButtonsModel.get(addedButtonsModel.size() - 1).getLayoutY() + 70 :
                245);
        addButton.setLayoutX(45);
        addButton.setOnAction(event -> showModel(addButton.getText()));
        addedButtonsModel.add(addButton);

        Button deleteButton = new Button("Удалить");
        deleteButton.setLayoutY(addedButtonsModel.get(addedButtonsModel.size() - 1).getLayoutY());
        deleteButton.setLayoutX(addedButtonsModel.get(addedButtonsModel.size() - 1).getLayoutX() + 85);
        deleteButton.setOnAction(event -> deleteModel(addButton.getText()));
        deletedButtonsModel.add(deleteButton);

        RadioButton radioButton = new RadioButton();
        radioButton.setLayoutY(deletedButtonsModel.get(deletedButtonsModel.size() - 1).getLayoutY() + 4);
        radioButton.setLayoutX(deletedButtonsModel.get(deletedButtonsModel.size() - 1).getLayoutX() + 75);
        radioButton.setOnAction(event -> showModel(addButton.getText()));
        choiceModelRadioButtons.add(radioButton);

        CheckBox checkBoxGrid = new CheckBox("Сетка");
        checkBoxGrid.setLayoutY(radioButton.getLayoutY() - 20);
        checkBoxGrid.setLayoutX(radioButton.getLayoutX() + 35);
        checkBoxGrid.setOnAction(event -> showGrid(addButton.getText()));
        checkBoxesGrid.add(checkBoxGrid);

        CheckBox checkBoxTexture = new CheckBox("Текстура");
        checkBoxTexture.setLayoutY(checkBoxGrid.getLayoutY() + 20);
        checkBoxTexture.setLayoutX(checkBoxGrid.getLayoutX());
        checkBoxTexture.setOnAction(event -> showTexture(addButton.getText()));
        checkBoxesTexture.add(checkBoxTexture);

        CheckBox checkBoxLighting = new CheckBox("Освещение");
        checkBoxLighting.setLayoutY(checkBoxTexture.getLayoutY() + 20);
        checkBoxLighting.setLayoutX(checkBoxTexture.getLayoutX());
        checkBoxLighting.setOnAction(event -> showLighting(addButton.getText()));
        checkBoxesLighting.add(checkBoxLighting);

        modelPane.getChildren().addAll(
                addButton, deleteButton, radioButton,
                checkBoxGrid, checkBoxTexture, checkBoxLighting
        );
    }

    @FXML
    private void createCamera() {
        if (!Objects.equals(eyeX.getText(), "") && !Objects.equals(eyeY.getText(), "") && !Objects.equals(eyeZ.getText(), "")
                && !Objects.equals(targetX.getText(), "") && !Objects.equals(targetY.getText(), "") && !Objects.equals(targetZ.getText(), "")) {
            for (Camera camera : cameras) {
                camera.setActive(false);
            }
            cameras.add(new Camera(
                    new Vector3f(Float.parseFloat(eyeX.getText()), Float.parseFloat(eyeY.getText()), Float.parseFloat(eyeZ.getText())),
                    new Vector3f(Float.parseFloat(targetX.getText()), Float.parseFloat(targetY.getText()), Float.parseFloat(targetZ.getText())),
                    1.0F, 1, 0.01F, 100, true));
            addCameraButtons();
        } else {
            showMessage("Предупреждение", "Введите необходимые данные!", messageWarning);
        }
    }

    public void showCamera(String text) {
        int numOfCamera = Integer.parseInt(text.substring(text.length() - 1));
        for (int i = 0; i < cameras.size(); i++) {
            cameras.get(i).setActive(false);
            if (i + 1 == numOfCamera) {
                cameras.get(i).setActive(true);
            }
        }
    }

    public void deleteCamera(String text) {
        int numOfCamera = Integer.parseInt(text.substring(text.length() - 1));
        for (int i = 0; i < addedButtonsCamera.size(); i++) {
            if (i + 1 == numOfCamera) {
                if (cameras.get(i).isActive()) {
                    showMessage("Информация", "Вы перенаправлены на: Камера 1", messageInformation);
                    cameras.get(0).setActive(true);
                }
                deleteCameraUI(i);
                break;
            }
        }
    }

    public void deleteCameraUI(int cameraID) {
        if (cameras.size() == 1) {
            showMessage("Ошибка", "Нельзя удалить единственную камеру!", messageError);
        } else {
            cameras.remove(cameraID);
            cameraPane.getChildren().remove(addedButtonsCamera.get(cameraID));
            cameraPane.getChildren().remove(deletedButtonsCamera.get(cameraID));
            for (int i = 0; i < addedButtonsCamera.size(); i++) {
                if (i + 1 > cameraID) {
                    addedButtonsCamera.get(i).setText("Камера " + i);
                }
            }
            for (int i = addedButtonsCamera.size() - 1; i >= 1; i--) {
                if (i + 1 > cameraID) {
                    addedButtonsCamera.get(i).setLayoutY(addedButtonsCamera.get(i - 1).getLayoutY());
                    deletedButtonsCamera.get(i).setLayoutY(deletedButtonsCamera.get(i - 1).getLayoutY());
                }
            }
            addedButtonsCamera.remove(cameraID);
            deletedButtonsCamera.remove(cameraID);
        }
    }

    public void convert(MouseEvent mouseEvent) {
        if (Objects.equals(tx.getText(), "") || Objects.equals(ty.getText(), "") || Objects.equals(tz.getText(), "")
                || Objects.equals(sx.getText(), "") || Objects.equals(sy.getText(), "") || Objects.equals(sz.getText(), "")
                || Objects.equals(rx.getText(), "") || Objects.equals(ry.getText(), "") || Objects.equals(rz.getText(), "")) {
            showMessage("Ошибка", "Введите необходимые данные!", messageError);
        } else {
            try {
                float txVal = Float.parseFloat(tx.getText());
                float tyVal = Float.parseFloat(ty.getText());
                float tzVal = Float.parseFloat(tz.getText());
                float rxVal = Float.parseFloat(rx.getText());
                float ryVal = Float.parseFloat(ry.getText());
                float rzVal = Float.parseFloat(rz.getText());
                float sxVal = Float.parseFloat(sx.getText());
                float syVal = Float.parseFloat(sy.getText());
                float szVal = Float.parseFloat(sz.getText());
                scene.transformActiveModel(txVal, tyVal, tzVal, rxVal, ryVal, rzVal, sxVal, syVal, szVal);
            } catch (NumberFormatException e) {
                showMessage("Ошибка", "Неправильный формат чисел!", messageError);
            } catch (RuntimeException ex) {
                showMessage("Ошибка", ex.getMessage(), messageError);
            }
        }
    }

    public void showModel(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        try {
            scene.setActiveModel(numOfModel - 1);
            for (RadioButton rb : choiceModelRadioButtons) {
                rb.setSelected(false);
            }
            choiceModelRadioButtons.get(numOfModel - 1).setSelected(true);
            Model activeM = scene.getMeshes().get(numOfModel - 1);
            baseModelColor.setValue(activeM.color);

        } catch (RuntimeException ex) {
            showMessage("Ошибка", ex.getMessage(), messageError);
        }
    }

    public void deleteModel(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        try {
            scene.deleteModel(numOfModel - 1);
        } catch (RuntimeException ex) {
            showMessage("Ошибка", ex.getMessage(), messageError);
            return;
        }
        modelPane.getChildren().remove(addedButtonsModel.get(numOfModel - 1));
        modelPane.getChildren().remove(deletedButtonsModel.get(numOfModel - 1));
        modelPane.getChildren().remove(choiceModelRadioButtons.get(numOfModel - 1));
        modelPane.getChildren().remove(checkBoxesGrid.get(numOfModel - 1));
        modelPane.getChildren().remove(checkBoxesTexture.get(numOfModel - 1));
        modelPane.getChildren().remove(checkBoxesLighting.get(numOfModel - 1));

        for (int i = 0; i < addedButtonsModel.size(); i++) {
            if (i + 1 > numOfModel) {
                addedButtonsModel.get(i).setText("Модель " + i);
            }
        }
        for (int i = addedButtonsModel.size() - 1; i >= 1; i--) {
            if (i + 1 > numOfModel) {
                addedButtonsModel.get(i).setLayoutY(addedButtonsModel.get(i - 1).getLayoutY());
                deletedButtonsModel.get(i).setLayoutY(deletedButtonsModel.get(i - 1).getLayoutY());
                choiceModelRadioButtons.get(i).setLayoutY(choiceModelRadioButtons.get(i - 1).getLayoutY());
                checkBoxesGrid.get(i).setLayoutY(checkBoxesGrid.get(i - 1).getLayoutY());
                checkBoxesTexture.get(i).setLayoutY(checkBoxesTexture.get(i - 1).getLayoutY());
                checkBoxesLighting.get(i).setLayoutY(checkBoxesLighting.get(i - 1).getLayoutY());
            }
        }

        addedButtonsModel.remove(numOfModel - 1);
        deletedButtonsModel.remove(numOfModel - 1);
        choiceModelRadioButtons.remove(numOfModel - 1);
        checkBoxesGrid.remove(numOfModel - 1);
        checkBoxesTexture.remove(numOfModel - 1);
        checkBoxesLighting.remove(numOfModel - 1);
    }

    public void showGrid(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        try {
            scene.toggleGrid(numOfModel - 1);
        } catch (RuntimeException ex) {
            showMessage("Ошибка", ex.getMessage(), messageError);
        }
    }

    public void showTexture(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        if (scene.getMeshes().get(numOfModel - 1).pathTexture == null) {
            FileChooser fileChooser = new FileChooser();
            fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Texture (*.png, *.jpg)", "*.png", "*.jpg"));
            fileChooser.setTitle("Load Texture");
            File file = fileChooser.showOpenDialog((Stage) canvas.getScene().getWindow());
            if (file == null) {
                try {
                    scene.toggleTexture(numOfModel - 1, null);
                } catch (RuntimeException ex) {
                    showMessage("Ошибка", ex.getMessage(), messageError);
                }
                return;
            }
            String path = file.getAbsolutePath();
            try {
                scene.toggleTexture(numOfModel - 1, path);
            } catch (RuntimeException ex) {
                showMessage("Ошибка", ex.getMessage(), messageError);
            }
        } else {
            try {
                scene.toggleTexture(numOfModel - 1, null);
            } catch (RuntimeException ex) {
                showMessage("Ошибка", ex.getMessage(), messageError);
            }
        }
    }

    public void showLighting(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        try {
            scene.toggleLighting(numOfModel - 1);
        } catch (RuntimeException ex) {
            showMessage("Ошибка", ex.getMessage(), messageError);
        }
    }

    public void changeDefaultColor(MouseEvent mouseEvent) {
        baseModelColor.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                Color c = baseModelColor.getValue();
                Model model = scene.getActiveModel();
                if (model != null) {
                    model.color = c;
                }
            }
        });
    }

    public void deleteVertexAction(MouseEvent mouseEvent) {
        try {
            scene.deleteVerticesFromActiveModel(deleteVertex.getText());
        } catch (RuntimeException ex) {
            showMessage("Ошибка", ex.getMessage(), messageError);
        }
    }

    @FXML
    public void handleMousePress(MouseEvent event) {
        if (event.isPrimaryButtonDown()) {
            isMousePressedForModel = true;
            lastMouseXForModel = event.getSceneX();
            lastMouseYForModel = event.getSceneY();
        }

        // После клика мышью захватываем фокус (чтобы хоткеи работали)
        canvas.requestFocus();
    }

    @FXML
    public void handleMouseDrag(MouseEvent event) {
        if (isMousePressedForModel) {
            double currentX = event.getSceneX();
            double currentY = event.getSceneY();

            double deltaX = currentX - lastMouseXForModel;
            double deltaY = currentY - lastMouseYForModel;

            float angleY = (float) (deltaX* rotationSensitivity * 0.01);
            float angleX = (float) (deltaY * rotationSensitivity * 0.01);

            Matrix4f rotationMatrix = AffineTransformations.rotationMatrix(angleX, -angleY, 0);
            Model model = scene.getActiveModel();
            if (model != null) {
                TranslationModel.move(rotationMatrix, model);
            }

            lastMouseXForModel = currentX;
            lastMouseYForModel = currentY;
        }
    }

    @FXML
    public void handleMouseRelease(MouseEvent event) {
        // Отпускаем левую кнопку
        if (isMousePressedForModel && event.isPrimaryButtonDown() == false) {
            isMousePressedForModel = false;
        }
    }

    @FXML
    public void handleScroll(ScrollEvent event) {
        float zoom = (float) event.getDeltaY() * zoomSpeed;
        activeCamera().movePosition(new Vector3f(0, 0, -zoom));
    }


}