package carleton.sysc4907.ui.view;

import carleton.sysc4907.DependencyInjector;
import carleton.sysc4907.command.Command;
import carleton.sysc4907.command.RemoveCommandFactory;
import carleton.sysc4907.command.args.RemoveCommandArgs;
import carleton.sysc4907.controller.DiagramMenuBarController;
import carleton.sysc4907.model.DiagramModel;
import carleton.sysc4907.processing.FileSaver;
import carleton.sysc4907.view.DiagramElement;
import javafx.application.Platform;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.property.BooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testfx.api.FxRobot;
import org.testfx.framework.junit5.ApplicationExtension;
import org.testfx.framework.junit5.Start;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ApplicationExtension.class)
public class DiagramMenuTest {

    @Mock
    private DiagramModel mockDiagramModel;

    @Mock
    private BooleanProperty mockIsSelectedElementsProperty;
    private ObservableList<DiagramElement> selectedElementsList = FXCollections.observableList(new LinkedList<>());
    @Mock
    private BooleanBinding booleanBinding;
    @Mock
    private RemoveCommandFactory mockRemoveCommandFactory;
    @Mock
    private FileSaver mockFileSaver;
    @Mock
    private DiagramElement mockElement;

    @Mock
    private DiagramElement mockElement2;
    @Mock
    private Command<RemoveCommandArgs> removeCommand;

    @Mock
    private File mockFile;

    private Pane root;

    @Start
    private void start(Stage stage) throws IOException {
        selectedElementsList.clear();
        selectedElementsList.add(mockElement);
        selectedElementsList.add(mockElement2);
        when(mockDiagramModel.getIsElementSelectedProperty()).thenReturn(mockIsSelectedElementsProperty);
        when(mockIsSelectedElementsProperty.not()).thenReturn(booleanBinding);
        DependencyInjector injector = new DependencyInjector();
        injector.addInjectionMethod(DiagramMenuBarController.class,
                () -> new DiagramMenuBarController(mockDiagramModel, mockRemoveCommandFactory, mockFileSaver));
        root = new Pane();
        root.setMinHeight(100);
        Scene scene = new Scene(root);
        root.getChildren().add(injector.load("view/DiagramMenu.fxml"));
        stage.setScene(scene);
        stage.show();
    }

    @Test
    public void testDeleteViaMnemonics(FxRobot robot) throws InterruptedException {
        when(mockDiagramModel.getSelectedElements()).thenReturn(selectedElementsList);
        when(mockRemoveCommandFactory.createTracked(any())).thenReturn(removeCommand);

        TimeUnit.MILLISECONDS.sleep(100);
        robot.press(KeyCode.ALT, KeyCode.E, KeyCode.D);
        TimeUnit.MILLISECONDS.sleep(100);

        verify(mockRemoveCommandFactory, times(1)).createTracked(any());
        verify(removeCommand, times(1)).execute();
    }

    @Test
    public void testDeleteViaKeybind(FxRobot robot) throws InterruptedException {
        when(mockDiagramModel.getSelectedElements()).thenReturn(selectedElementsList);
        when(mockRemoveCommandFactory.createTracked(any())).thenReturn(removeCommand);
        when(mockIsSelectedElementsProperty.get()).thenReturn(true);

        Platform.runLater(() -> root.requestFocus());
        TimeUnit.MILLISECONDS.sleep(300);
        robot.press(KeyCode.DELETE);
        TimeUnit.MILLISECONDS.sleep(100);

        verify(mockRemoveCommandFactory).createTracked(any());
        verify(removeCommand).execute();
    }

    @Test
    public void testSaveViaMnemonicsWhenAlreadySaved(FxRobot robot) throws InterruptedException {
        when(mockFileSaver.save()).thenReturn(true);
        when(mockDiagramModel.getLoadedFilePath()).thenReturn("notNull");

        TimeUnit.MILLISECONDS.sleep(100);
        robot.press(KeyCode.ALT, KeyCode.F, KeyCode.S);
        TimeUnit.MILLISECONDS.sleep(100);

        verify(mockFileSaver).save();
    }

    @Test
    public void testSaveViaKeybindWhenAlreadySaved(FxRobot robot) throws InterruptedException {
        when(mockFileSaver.save()).thenReturn(true);
        when(mockDiagramModel.getLoadedFilePath()).thenReturn("notNull");

        Platform.runLater(() -> root.requestFocus());
        TimeUnit.MILLISECONDS.sleep(300);
        robot.press(KeyCode.CONTROL, KeyCode.S);
        TimeUnit.MILLISECONDS.sleep(100);

        verify(mockFileSaver).save();
    }
}