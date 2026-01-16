import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

class EventHandlerNewStatus implements EventHandler<ActionEvent>
{
    private Bowling bowling;

    public EventHandlerNewStatus(Bowling bowling)
    {
        this.bowling = bowling;
    }

    @Override
    public void handle(ActionEvent actionEvent)
    {
        new Status(this.bowling);
    }
}

public class Bowlingbanen extends Application {

    private Bowling bowling;

    public Bowlingbanen () {
        this.bowling = new Bowling ();
    }

    @Override
    public void start(Stage primaryStage)
    {
        Pane rootPane = new Pane();
        rootPane.setMinSize(400, 200);
        Scene startScene = new Scene(rootPane);

        Button button = new Button("Open new status window");
        button.setOnAction(new EventHandlerNewStatus(bowling));
        rootPane.getChildren().add(button);

        primaryStage.setScene(startScene);
        primaryStage.setTitle("Bowling lanes");
        primaryStage.show();
    }
}