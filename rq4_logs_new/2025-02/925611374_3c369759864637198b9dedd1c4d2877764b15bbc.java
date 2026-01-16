package com.cgvsu;

import com.cgvsu.math.AffineTransformations;
import com.cgvsu.math.TranslationModel;
import com.cgvsu.model.DeleteVertices;
import com.cgvsu.ObjWriter.ObjWriter;
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
import javafx.scene.layout.AnchorPane;
import javafx.scene.paint.Color;
import javafx.stage.Stage;
import javafx.stage.FileChooser;
import javafx.util.Duration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.vecmath.Matrix4f;
import javax.vecmath.Vector3f;

import com.cgvsu.model.Model;
import com.cgvsu.objreader.ObjReader;
import com.cgvsu.render_engine.Camera;

public class GuiController {

    final private float TRANSLATION = 0.9F; //шаг перемещения камеры

    public ColorPicker baseModelColor = new ColorPicker();
    public CheckBox transformSave;

    //для модели
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

    Alert messageWarning = new Alert(Alert.AlertType.WARNING);
    Alert messageError = new Alert(Alert.AlertType.ERROR);
    Alert messageInformation = new Alert(Alert.AlertType.INFORMATION);

    @FXML
    public Button open;
    @FXML
    public Button save;
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
    AnchorPane anchorPane;

    @FXML
    private Canvas canvas;

    //список камер
    private List<Camera> cameras = new ArrayList<>();
    //кнопки  камер
    private List<Button> addedButtonsCamera = new ArrayList<>();
    //кнопки удаления камер
    private List<Button> deletedButtonsCamera = new ArrayList<>();

    //список моделей
    private List<Model> meshes = new ArrayList<>();
    //кнопки моделей
    private List<Button> addedButtonsModel = new ArrayList<>();
    private List<CheckBox> checkBoxesTexture = new ArrayList<>();
    private List<CheckBox> checkBoxesLighting = new ArrayList<>();
    private List<CheckBox> checkBoxesGrid = new ArrayList<>();
    private List<RadioButton> choiceModelRadioButtons = new ArrayList<>();
    //кнопки удаления моделей
    private List<Button> deletedButtonsModel = new ArrayList<>();


    private Timeline timeline;

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

            if (meshes.size() != 0) {
                RenderEngine.render(canvas.getGraphicsContext2D(), activeCamera(), meshes, (int) width, (int) height); //создаем отрисовку модели

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
        showMessage("Осторожно", "Переключено на  Камеру 1", messageInformation);
        return cameras.get(0);
    }

    @FXML
    public void moveCamera(KeyEvent keyEvent) {
        if (Objects.equals(keyEvent.getText(), "w")) {
            activeCamera().movePosition(new Vector3f(0, 0, -TRANSLATION));
        }
        if (Objects.equals(keyEvent.getText(), "s")) {
            activeCamera().movePosition(new Vector3f(0, 0, TRANSLATION));
        }
        if (Objects.equals(keyEvent.getText(), "a")) {
            activeCamera().movePosition(new Vector3f(TRANSLATION, 0, 0));
        }
        if (Objects.equals(keyEvent.getText(), "d")) {
            activeCamera().movePosition(new Vector3f(-TRANSLATION, 0, 0));
        }
        if (Objects.equals(keyEvent.getText(), "r")) {
            activeCamera().movePosition(new Vector3f(0, TRANSLATION, 0));
        }
        if (Objects.equals(keyEvent.getText(), "f")) {
            activeCamera().movePosition(new Vector3f(0, -TRANSLATION, 0));
        }
        if (Objects.equals(keyEvent.getText(), "ц")) {
            activeCamera().movePosition(new Vector3f(0, 0, -TRANSLATION));
        }
        if (Objects.equals(keyEvent.getText(), "ы")) {
            activeCamera().movePosition(new Vector3f(0, 0, TRANSLATION));
        }
        if (Objects.equals(keyEvent.getText(), "ф")) {
            activeCamera().movePosition(new Vector3f(TRANSLATION, 0, 0));
        }
        if (Objects.equals(keyEvent.getText(), "в")) {
            activeCamera().movePosition(new Vector3f(-TRANSLATION, 0, 0));
        }
        if (Objects.equals(keyEvent.getText(), "к")) {
            activeCamera().movePosition(new Vector3f(0, TRANSLATION, 0));
        }
        if (Objects.equals(keyEvent.getText(), "а")) {
            activeCamera().movePosition(new Vector3f(0, -TRANSLATION, 0));
        }
    }

    @FXML
    void open(MouseEvent event) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Model (*.obj)", "*.obj"));
        fileChooser.setTitle("Load Model");

        File file = fileChooser.showOpenDialog((Stage) canvas.getScene().getWindow());
        if (file == null) {
            return;
        }

        Path fileName = Path.of(file.getAbsolutePath());

        try {
            String fileContent = Files.readString(fileName);
            Model mesh = ObjReader.read(fileContent);
            mesh.triangulate();
            mesh.normalize();
            meshes.add(mesh);
            addModelButtons();
        } catch (IOException exception) {
            showMessage("Ошибка", "Неудалось найти файл!", messageError);
        }
    }

    @FXML
    void save(MouseEvent event) {
        if (meshes.size() != 0) {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Save Model");

            File file = fileChooser.showSaveDialog((Stage) canvas.getScene().getWindow());
            if (file == null) {
                return;
            }
            String fileName = String.valueOf(Path.of(file.getAbsolutePath()));
            if (transformSave.isSelected()) {

            }
            ObjWriter.write(activeModel(), (fileName.substring(fileName.length() - 4).equals(".obj")) ? fileName : fileName + ".obj");
            showMessage("Информация", "Модель успешно сохранёна!", messageInformation);
        } else {
            showMessage("Предупреждение", "Откройте модель для сохранения!", messageWarning);
        }
    }

    public void addCameraButtons() {
        //кнопка добавления камеры
        Button addButton = new Button("Камера " + (addedButtonsCamera.size() + 1));
        addButton.setLayoutY((addedButtonsCamera.size() > 0) ?
                addedButtonsCamera.get(addedButtonsCamera.size() - 1).getLayoutY() + 40 :
                185);
        addButton.setLayoutX(33);
        addButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                showCamera(addButton.getText());
            }
        });
        addedButtonsCamera.add(addButton);

        //кнопка удаления камеры
        Button deleteButton = new Button("Удалить");
        deleteButton.setLayoutY(addedButtonsCamera.get(addedButtonsCamera.size() - 1).getLayoutY());
        deleteButton.setLayoutX(addedButtonsCamera.get(addedButtonsCamera.size() - 1).getLayoutX() + 85);
        deleteButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                deleteCamera(addButton.getText());
            }
        });
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
        addButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                showModel(addButton.getText());
            }
        });
        addedButtonsModel.add(addButton);

        Button deleteButton = new Button("Удалить");
        deleteButton.setLayoutY(addedButtonsModel.get(addedButtonsModel.size() - 1).getLayoutY());
        deleteButton.setLayoutX(addedButtonsModel.get(addedButtonsModel.size() - 1).getLayoutX() + 85);
        deleteButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                deleteModel(addButton.getText());
            }
        });
        deletedButtonsModel.add(deleteButton);

        RadioButton radioButton = new RadioButton();
        radioButton.setLayoutY(deletedButtonsModel.get(deletedButtonsModel.size() - 1).getLayoutY() + 4);
        radioButton.setLayoutX(deletedButtonsModel.get(deletedButtonsModel.size() - 1).getLayoutX() + 75);
        radioButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                showModel(addButton.getText());
            }
        });
        choiceModelRadioButtons.add(radioButton);

        showModel(addButton.getText());

        //Сетка
        CheckBox checkBoxGrid = new CheckBox("Сетка");
        checkBoxGrid.setLayoutY(choiceModelRadioButtons.get(choiceModelRadioButtons.size() - 1).getLayoutY() - 20);
        checkBoxGrid.setLayoutX(choiceModelRadioButtons.get(choiceModelRadioButtons.size() - 1).getLayoutX() + 35);
        checkBoxGrid.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                showGrid(addButton.getText());
            }
        });
        checkBoxesGrid.add(checkBoxGrid);

        //Тексутры
        CheckBox checkBoxTexture = new CheckBox("Текстура");
        checkBoxTexture.setLayoutY(checkBoxesGrid.get(checkBoxesGrid.size() - 1).getLayoutY() + 20);
        checkBoxTexture.setLayoutX(checkBoxesGrid.get(checkBoxesGrid.size() - 1).getLayoutX());
        checkBoxTexture.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                showTexture(addButton.getText());
            }
        });
        checkBoxesTexture.add(checkBoxTexture);

        // Освещение
        CheckBox checkBoxLighting = new CheckBox("Освещение");
        checkBoxLighting.setLayoutY(checkBoxesTexture.get(checkBoxesTexture.size() - 1).getLayoutY() + 20);
        checkBoxLighting.setLayoutX(checkBoxesTexture.get(checkBoxesTexture.size() - 1).getLayoutX());
        checkBoxLighting.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                showLighting(addButton.getText());
            }
        });
        checkBoxesLighting.add(checkBoxLighting);

        modelPane.getChildren().add(addButton);
        modelPane.getChildren().add(deleteButton);
        modelPane.getChildren().add(radioButton);
        modelPane.getChildren().add(checkBoxGrid);
        modelPane.getChildren().add(checkBoxTexture);
        modelPane.getChildren().add(checkBoxLighting);
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
            if (cameras.get(i).isActive()) {
                cameras.get(i).setActive(false);
            }
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
                delete(i);
                break;
            }
        }
    }

    public void delete(int cameraID) {
        if (cameras.size() == 1) {
            showMessage("Ошибка", "Нельзя удалить единственную камеру!", messageError);
        } else {
            cameras.remove(cameraID);
            cameraPane.getChildren().remove(addedButtonsCamera.get(cameraID));
            cameraPane.getChildren().remove(deletedButtonsCamera.get(cameraID));
            //переименовываем кнопки
            for (int i = 0; i < addedButtonsCamera.size(); i++) {
                if (i + 1 > cameraID) {
                    addedButtonsCamera.get(i).setText("Камера " + i);
                }
            }
            //смещаем координаты
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
        //реализовываю только для смещения
        if (Objects.equals(tx.getText(), "") || Objects.equals(ty.getText(), "") || Objects.equals(tz.getText(), "")
                || Objects.equals(sx.getText(), "") || Objects.equals(sy.getText(), "") || Objects.equals(sz.getText(), "")
                || Objects.equals(rx.getText(), "") || Objects.equals(ry.getText(), "") || Objects.equals(rz.getText(), "")) {
            showMessage("Ошибка", "Введите необходимые данные!", messageError);
        } else {
            Matrix4f transposeMatrix = AffineTransformations.modelMatrix(
                    Integer.parseInt(tx.getText()), Integer.parseInt(ty.getText()), Integer.parseInt(tz.getText()),
                    Float.parseFloat(rx.getText()), Float.parseFloat(ry.getText()), Float.parseFloat(rz.getText()),
                    Integer.parseInt(sx.getText()), Integer.parseInt(sy.getText()), Integer.parseInt(sz.getText()));
            TranslationModel.move(transposeMatrix, activeModel());
        }
    }

    private Model activeModel() {
        for (Model mesh : meshes) {
            if (mesh.isActive) {
                return mesh;
            }
        }
        showMessage("Предупреждение", "Активных моделей нет! Выбрана первая", messageWarning);
        return meshes.get(0);
    }

    public void showModel(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        for (int i = 0; i < meshes.size(); i++) {
            if (meshes.get(i).isActive) {
                meshes.get(i).isActive = false;
                choiceModelRadioButtons.get(i).setSelected(false);
            }
            if (i + 1 == numOfModel) {
                meshes.get(i).isActive = true;
                choiceModelRadioButtons.get(i).setSelected(true);
                baseModelColor.setValue(meshes.get(i).color);
            }
        }
    }

    public void deleteModel(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        meshes.remove(numOfModel - 1);
        modelPane.getChildren().remove(addedButtonsModel.get(numOfModel - 1));
        modelPane.getChildren().remove(deletedButtonsModel.get(numOfModel - 1));
        modelPane.getChildren().remove(choiceModelRadioButtons.get(numOfModel - 1));
        modelPane.getChildren().remove(checkBoxesGrid.get(numOfModel - 1));
        modelPane.getChildren().remove(checkBoxesTexture.get(numOfModel - 1));
        modelPane.getChildren().remove(checkBoxesLighting.get(numOfModel - 1));
        //переименовываем кнопки
        for (int i = 0; i < addedButtonsModel.size(); i++) {
            if (i + 1 > numOfModel) {
                addedButtonsModel.get(i).setText("Модель " + i);
            }
        }
        //смещаем координаты
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
        for (int i = 0; i < meshes.size(); i++) {
            if (i + 1 == numOfModel) {
                if (meshes.get(i).isActiveGrid) {
                    meshes.get(i).isActiveGrid = false;
                    checkBoxesGrid.get(i).setSelected(false);
                } else {
                    meshes.get(i).isActiveGrid = true;
                    checkBoxesGrid.get(i).setSelected(true);
                }
            }
        }
    }

    public void showTexture(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        for (int i = 0; i < meshes.size(); i++) {
            if (i + 1 == numOfModel) {
                if (meshes.get(i).pathTexture == null) {
                    FileChooser fileChooser = new FileChooser();
                    fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Texture (*.png, *.jpg)", "*.png", "*.jpg"));
                    fileChooser.setTitle("Load Texture");

                    File file = fileChooser.showOpenDialog((Stage) canvas.getScene().getWindow());
                    if (file == null) {
                        meshes.get(i).isActiveTexture = false;
                        checkBoxesTexture.get(i).setSelected(false);
                        return;
                    }

                    Path fileName = Path.of(file.getAbsolutePath());
                    meshes.get(i).pathTexture = String.valueOf(fileName);
                }
                if (meshes.get(i).isActiveTexture) {
                    meshes.get(i).isActiveTexture = false;
                    checkBoxesTexture.get(i).setSelected(false);
                } else {
                    meshes.get(i).isActiveTexture = true;
                    checkBoxesTexture.get(i).setSelected(true);
                }

            }
        }

    }

    public void showLighting(String text) {
        int numOfModel = Integer.parseInt(text.substring(text.length() - 1));
        for (int i = 0; i < meshes.size(); i++) {
            if (i + 1 == numOfModel) {
                if (meshes.get(i).isActiveLighting) {
                    meshes.get(i).isActiveLighting = false;
                    checkBoxesLighting.get(i).setSelected(false);
                } else {
                    meshes.get(i).isActiveLighting = true;
                    checkBoxesLighting.get(i).setSelected(true);
                }
            }
        }
    }

    public void changeDefaultColor(MouseEvent mouseEvent) {
        baseModelColor.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                Color c = baseModelColor.getValue();
                Model model = activeModel();
                model.color = c;
            }
        });
    }

    public void deleteVertexAction(MouseEvent mouseEvent) {
        List<Integer> vectorIndex = parseVertex(deleteVertex.getText());
        if (vectorIndex.size() == 0) {
            showMessage("Ошибка", "нет такой вершины у активной модели", messageError);
        } else {
            Model model = activeModel();
            model = DeleteVertices.deleteVerticesFromModel(activeModel(), vectorIndex);
            model.normalize();
        }
    }

    public List<Integer> parseVertex(String vertex) {
        List<Float> coords = new ArrayList<>();
        StringBuilder prom = new StringBuilder();
        for (int i = 0; i < vertex.length(); i++) {
            if (vertex.charAt(i) == ' ') {
                coords.add(Float.parseFloat(String.valueOf(prom)));
                prom = new StringBuilder();
            } else {
                prom.append(vertex.charAt(i));
            }
        }
        coords.add(Float.parseFloat(String.valueOf(prom)));
        com.cgvsu.math.Vector3f resultVector = new com.cgvsu.math.Vector3f(coords.get(0), coords.get(1), coords.get(2));
        Model model = activeModel();
        for (int i = 0; i < model.vertices.size(); i++) {
            if (model.vertices.get(i).equals(resultVector)) {
                return new ArrayList<>(List.of(i));
            }
        }
        return new ArrayList<>();
    }
}