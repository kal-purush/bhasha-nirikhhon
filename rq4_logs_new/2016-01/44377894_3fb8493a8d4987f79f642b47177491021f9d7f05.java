package javafxgui.javafxcomponents;

import javafx.embed.swing.JFXPanel;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafxgui.GUIViewSpy;
import javafxgui.event.EventRegister;
import javafxgui.javafxcomponents.JFXGameOptionsPage;
import javafxgui.view.BoardDisplay;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JavaFxGameOptionsPageTest {
    private static final int WINDOW_HEIGHT = 700;
    private static final int WINDOW_WIDTH = 600;

    @Before
    public void setUp() {
        new JFXPanel();
    }

    @Test
    @Ignore
    public void pageContainsButtonForEachGameType() {
        Scene scene = new Scene(new StackPane(), WINDOW_HEIGHT, WINDOW_WIDTH);
        JFXGameOptionsPage gameOptionsPage = new JFXGameOptionsPage(
                new GUIViewSpy(scene, new BoardDisplay(), new EventRegister()));
        assertEquals(6, gameOptionsPage.getChildren().size());

    }
}