package interp;

import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Spinner;
import javafx.scene.layout.GridPane;
import javafx.stage.Modality;
import javafx.stage.Stage;

/**
 * This class creates a window for get number of months
 * whether to do most popular or least popular radios
 * as well as whether to include 0ed inventory or not
 */
public class PopularInfo {
    static int monthNum;
    static boolean popular;
    static boolean skipZero;

    static boolean madeWindow;
    public static void makeWindow(){
        Stage stage = new Stage();
        stage.initModality(Modality.APPLICATION_MODAL);
        stage.setTitle("Popularity Info");

        stage.setHeight(100);
        stage.setWidth(200);

        GridPane layout = new GridPane();

        Spinner numbers = new Spinner<Integer>(1,12,1);
        numbers.setEditable(true);

        Button close = new Button("OK");

        close.setOnMouseClicked(e->{
            stage.close();
        });

        layout.setVgap(5);
        layout.setHgap(10);

        layout.add(numbers,1,3);
        layout.add(close,1,2);
        layout.setAlignment(Pos.CENTER);

        Scene scene = new Scene(layout,400,200);

        stage.setScene(scene);
        madeWindow = true;


        stage.showAndWait();
    }
}