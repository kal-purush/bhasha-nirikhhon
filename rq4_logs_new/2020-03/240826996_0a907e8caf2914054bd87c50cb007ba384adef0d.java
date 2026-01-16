import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;
public class EditorPane extends StackPane{

    private StackPane pane;
    public EditorPane(Stage ps){
        pane = new StackPane();
        Label editor_l = new Label("Editor");

        pane.getChildren().addAll(editor_l);

    }


    public StackPane getPane(){
        return pane;
    }

}