package service;

import data.components.interfaces.IQuestionComponent;
import data.interfaces.IQuestion;
import javafx.scene.Node;
import javafx.scene.Parent;
import service.interfaces.IGUIConverter;

/**
 * Convert Question Data to JavaFX Components
 */
public class GUIConverter implements IGUIConverter {

    /**
     * convert any QuestionComponent into JavaFX Nodes.
     * Each QuestionComponent needs its own Code.
     *
     * @param component Question Component
     * @return JavaFX Node
     */
    @Override
    public Node convertQuestionComponent(IQuestionComponent component) {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    /**
     * convert a Question to its JavaFX QuestionPane
     *
     * @param question Question
     * @return JavaFX Parent
     */
    @Override
    public Parent convertQuestion(IQuestion question) {
        // TODO implement
        throw new UnsupportedOperationException();
    }

}