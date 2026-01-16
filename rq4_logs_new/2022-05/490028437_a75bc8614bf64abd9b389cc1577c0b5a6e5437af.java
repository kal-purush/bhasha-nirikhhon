package opt3.Controller;

import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.layout.HBox;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import opt3.JavaApplication;
import opt3.Model.*;

import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.event.ActionEvent;
import javafx.scene.layout.VBox;

public class AddController {

    private static AnchorPane field;

    @FXML
    public static void start(ActionEvent e, Stage stage) throws IOException{
        AnchorPane root = FXMLLoader.load(JavaApplication.class.getResource("/opt3/Add.fxml"));
        Scene scene = new Scene( root );
        stage.setScene(scene);
        Object obj = stage.getUserData();
        User user = (User)obj;
        stage.setUserData(user);
        Label text = new Label("welkom " + user.getUsername());
        text.setFont(Font.font("Arial"));
        text.setPadding(new Insets(20));
        root.getChildren().addAll(text);
        field = root;
    }

    @FXML
    public static void setAdd(ActionEvent event, String type, Stage stage){
        Button back = new Button("Terug");
        back.setFont(Font.font("Arial"));
        back.setOnAction(e -> {
            try {
                Menu(event, stage);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });

        VBox vbox = new VBox();

        Text typeText = new Text(type);
        typeText.setFont(Font.font("Arial"));

        HBox nameBox = new HBox();
        Label nameLabel = new Label("Naam");
        nameLabel.setFont(Font.font("Arial"));
        TextField name = new TextField();
        name.setFont(Font.font("Arial"));
        nameBox.getChildren().setAll(nameLabel, name);
        nameBox.setSpacing(10);

        HBox descriptionBox = new HBox();
        Label descriptionLabel = new Label("Beschrijving");
        descriptionLabel.setFont(Font.font("Arial"));
        TextField description = new TextField();
        description.setFont(Font.font("Arial"));
        descriptionBox.getChildren().setAll(descriptionLabel, description);
        descriptionBox.setSpacing(10);

        HBox priceBox = new HBox();
        Label priceLabel = new Label("Prijs");
        priceLabel.setFont(Font.font("Arial"));
        TextField price = new TextField();
        price.setFont(Font.font("Arial"));
        priceBox.getChildren().setAll(priceLabel, price);
        priceBox.setSpacing(10);

        HBox brandBox = new HBox();
        Label brandLabel = new Label("Merk");
        brandLabel.setFont(Font.font("Arial"));
        TextField brand = new TextField();
        brand.setFont(Font.font("Arial"));
        if (type.equals("Personenauto's") || type.equals("Boormachines")) {
            brandBox.getChildren().setAll(brandLabel, brand);
            brandBox.setSpacing(10);
        }

        HBox drillTypeBox = new HBox();
        Label drillTypeLabel = new Label("Type");
        drillTypeLabel.setFont(Font.font("Arial"));
        TextField drillType = new TextField();
        drillType.setFont(Font.font("Arial"));
        if (type.equals("Boormachines")) {
            drillTypeBox.getChildren().setAll(drillTypeLabel, drillType);
            drillTypeBox.setSpacing(10);
        }

        HBox dayPriceBox = new HBox();
        Label dayPriceLabel = new Label("Dagprijs");
        dayPriceLabel.setFont(Font.font("Arial"));
        Text priceDay = new Text("0");
        if (type.equals("Personenauto's")){
                priceDay = new Text("50");
        }
        if (type.equals("Boormachines")){
            priceDay = new Text("5");
        }
        priceDay.setFont(Font.font("Arial"));
        priceDay.setDisable(true);
        dayPriceBox.getChildren().setAll(dayPriceLabel, priceDay);
        dayPriceBox.setSpacing(10);

        HBox insuranceBox = new HBox();
        Label insuranceLabel = new Label("Verzekering");
        insuranceLabel.setFont(Font.font("Arial"));
        Text insurance = new Text("0");
        if (type.equals("Boormachines")){
            insurance = new Text("1");
        }
        insurance.setFont(Font.font("Arial"));
        insurance.setDisable(true);
        insuranceBox.getChildren().setAll(insuranceLabel, insurance);
        insuranceBox.setSpacing(10);

        HBox weightBox = new HBox();
        Label weightLabel = new Label("Gewicht in kg");
        weightLabel.setFont(Font.font("Arial"));
        TextField weight = new TextField();
        weight.textProperty().addListener( e -> {
            if (type.equals("Vrachtwagen") || type.equals("Personenauto's")) {
                Text newText = new Text();
                if(!weight.getText().equals("")) {
                    newText = new Text("" + (Integer.parseInt(weight.getText()) * 0.01));
                }
                newText.setFont(Font.font("Arial"));
                insuranceBox.getChildren().setAll(insuranceLabel, newText);
            }
        });
        weight.setFont(Font.font("Arial"));
        if (type.equals("Personenauto's") || type.equals("Vrachtwagen")) {

            weightBox.getChildren().setAll(weightLabel, weight);
            weightBox.setSpacing(10);
        }

        HBox loadBox = new HBox();
        Label loadLabel = new Label("Laadgewicht in kg");
        loadLabel.setFont(Font.font("Arial"));
        TextField load = new TextField();
        load.textProperty().addListener( e -> {
            if (type.equals("Vrachtwagen")) {
                Text newText = new Text();
                if(!load.getText().equals("")){
                    newText = new Text("" + (Integer.parseInt(load.getText()) * 0.10));
                }
                newText.setFont(Font.font("Arial"));
                dayPriceBox.getChildren().setAll(dayPriceLabel, newText);
            }
        });
        load.setFont(Font.font("Arial"));
        if (type.equals("Personenauto's") || type.equals("Vrachtwagen")) {

            loadBox.getChildren().setAll(loadLabel, load);
            loadBox.setSpacing(10);
        }

        HBox addBox = new HBox();
        Label addLabel = new Label("Toevoegen");
        addLabel.setFont(Font.font("Arial"));
        Button add = new Button("Voeg toe");
        add.setFont(Font.font("Arial"));
        add.setOnAction(e -> {
            Label success = new Label();
            if (type.equals("Personenauto's")) {
                Item.items.add(new Car(brand.getText(), Integer.parseInt(weight.getText()), name.getText(), description.getText(), Integer.parseInt(price.getText()), false));
                success = new Label("Auto is toegevoegd! druk op terug om af te ronden");
            }
            if (type.equals("Vrachtwagen")) {
                Item.items.add(new Truck(Integer.parseInt(load.getText()), Integer.parseInt(weight.getText()), name.getText(), description.getText(), Integer.parseInt(price.getText()), false));
                success = new Label("Vrachtwagen is toegevoegd! druk op terug om af te ronden");
            }
            if (type.equals("Boormachines")) {
                Item.items.add(new Drill(brand.getText(), drillType.getText(), name.getText(), description.getText(), Integer.parseInt(price.getText()), false));
                success = new Label("Boormachine is toegevoegd! druk op terug om af te ronden");
            }

            success.setFont(Font.font("Arial"));
            success.setPadding(new Insets(20));
            field.getChildren().addAll(success);
        });
        addBox.getChildren().setAll(addLabel, add);
        addBox.setSpacing(10);


        vbox.setPadding(new Insets(90, 100 , 100, 170));
        vbox.getChildren().addAll(back, typeText, nameBox, descriptionBox, priceBox, brandBox, drillTypeBox, weightBox, loadBox, dayPriceBox, insuranceBox, addBox);
        vbox.setSpacing(10);
        field.getChildren().addAll(vbox);
    }

    @FXML
    public static void Menu(ActionEvent e, Stage stage) throws IOException{
        AnchorPane root = FXMLLoader.load(JavaApplication.class.getResource("/opt3/Menu.fxml"));
        Scene scene = new Scene( root );
        stage.setScene(scene);
        Object obj = stage.getUserData();
        User user = (User)obj;
        stage.setUserData(user);
        Label text = new Label("welkom " + user.getUsername());
        text.setFont(Font.font("Arial"));
        text.setPadding(new Insets(20));
        root.getChildren().addAll(text);
        field = root;
    }
}