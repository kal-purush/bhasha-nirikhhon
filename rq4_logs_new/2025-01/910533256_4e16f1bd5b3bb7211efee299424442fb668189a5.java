package tictactoe.domain.usecases;

import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

public class ShowPopupUseCase {

    public static void showPopup(Stage currentStage, Stage newStage, int newStageHalfWidth) {
        newStage.initModality(Modality.APPLICATION_MODAL);
        newStage.initOwner(currentStage);
        newStage.setX(currentStage.getX() + (currentStage.getWidth() / 2) - newStageHalfWidth);
        newStage.setY(currentStage.getY() + (currentStage.getHeight() / 2) - newStageHalfWidth);
        newStage.initStyle(StageStyle.UNDECORATED);
        newStage.show();
    }

}