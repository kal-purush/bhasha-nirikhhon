package se.femtearenan.mindmap.ui;

import javafx.scene.Node;
import javafx.scene.control.Dialog;
import javafx.scene.control.TextInputDialog;
import javafx.scene.input.ContextMenuEvent;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Shape;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import se.femtearenan.mindmap.model.Bubble;
import se.femtearenan.mindmap.model.BubbleType;
import se.femtearenan.mindmap.model.Idea;
import se.femtearenan.mindmap.model.IdeaConnectionType;
import se.femtearenan.mindmap.utility.SelectionState;
import se.femtearenan.mindmap.utility.SizeChoice;

import java.util.Optional;

class ContextMenuActions {

    private IdeaController ideaController;
    private ContextMenuController contextMenuController;
    private Idea manipulatedIdea;

    ContextMenuActions(IdeaController ideaController, ContextMenuController contextMenuController) {
        this.ideaController = ideaController;
        this.contextMenuController = contextMenuController;
    }

    void optionCreate(ContextMenuEvent event) {
        double sceneX = event.getSceneX();
        double sceneY = event.getSceneY();

        optionCreateThoughtAt(sceneX, sceneY);
    }

    private void optionCreateThoughtAt(double x, double y) {
        String theme;
        Idea idea;

        // on right click, open a dialog to take text; the theme for Idea
        double width = 280;
        Dialog dialog = new TextInputDialog();
        dialog.setX(x + width);
        dialog.setY(y + 60);
        dialog.setHeaderText("");
        dialog.setTitle("Add an idea");
        dialog.setContentText("Enter an idea:");
        Optional result = dialog.showAndWait();

        // create the Idea with the info of a theme
        // create a basic Bubble (Ellipse shape)
        if (result.isPresent() && ((String)result.get()).length() > 0) {
            theme = (String)result.get();
            if (ideaController.getIdeaByTheme(theme) == null) {
                idea = new Idea(theme);
                Pane pane = ideaController.ideaToPane(idea);
                pane.setTranslateX(x);
                pane.setTranslateY(y);
                ideaController.getIdeaGroup().getChildren().add(pane);
            } else {
                System.out.println("Already exists!\n");
            }
        }

        // add options upon right clicking the Bubble;
        // shape, color, sizes

        // add options upon right clicking to remove, create child or update

    }

    void optionSetParent(ContextMenuEvent event) {
        Pane pane = (Pane) event.getSource();
        manipulatedIdea = ideaController.getIdeaFromPane(pane);
        contextMenuController.setSelectionState(SelectionState.SELECT_PARENT);
    }

    void optionSetAcquaintance(ContextMenuEvent event) {
        Pane pane = (Pane) event.getSource();
        manipulatedIdea = ideaController.getIdeaFromPane(pane);
        contextMenuController.setSelectionState(SelectionState.SELECT_ACQUAINTANCE);
    }

    void optionRemoveConnection(ContextMenuEvent event) {
        Pane pane = (Pane) event.getSource();
        manipulatedIdea = ideaController.getIdeaFromPane(pane);
        contextMenuController.setSelectionState(SelectionState.REMOVE_CONNECTION);
    }

    void optionDeleteIdea(ContextMenuEvent event) {
        Pane pane = (Pane) event.getSource();
        ideaController.deleteIdea(pane);
    }

    void optionChangeShape(ContextMenuEvent event, BubbleType bubbleType) {
        if (event.getSource() instanceof Pane) {
            Pane pane = (Pane)event.getSource();
            Idea idea = ideaController.getIdeaFromPane(pane);
            Bubble bubble = idea.getBubble();
            bubble.setType(bubbleType);

            Shape originalShape = ideaController.getShapeFromPane(pane);
            Shape newShape = bubble.getShape();

            replaceNodeOnPane(pane, originalShape, newShape);
        }
    }

    void optionSetShapeColor(ContextMenuEvent event, Color color) {
        if (event.getSource() instanceof Pane) {
            Pane pane = (Pane)event.getSource();
            Idea idea = ideaController.getIdeaFromPane(pane);
            Bubble bubble = idea.getBubble();
            bubble.setColor(color);

            Shape shape = ideaController.getShapeFromPane(pane);
            shape.setStroke(color);
        }
    }

    void optionSetShapeSize(ContextMenuEvent event, boolean larger) {

        Pane pane = ideaController.getPaneFromEvent(event);
        Shape shape = ideaController.getShapeFromPane(pane);
        Idea idea = ideaController.getIdeaFromPane(pane);
        Bubble bubble = idea.getBubble();

        int sizeX = bubble.getSizeX();
        int sizeY = bubble.getSizeY();

        int incrementX = 10;
        int incrementY = 4;

        if (larger) {
            sizeX += incrementX;
            sizeY += incrementY;
        } else {
            sizeX -= incrementX;
            sizeY -= incrementY;
        }

        bubble.setSizeY(sizeY);
        bubble.setSizeX(sizeX);

        Node newNode = bubble.getShape();

        replaceNodeOnPane(pane, shape, newNode);

    }

    private void replaceNodeOnPane(Pane pane, Node paneNode, Node replaceWith) {

        int zIndex = 0;

        for (int i = 0; i < pane.getChildren().size(); i++) {
            if (pane.getChildren().get(i) == paneNode) {
                zIndex = i;
            }
        }

        pane.getChildren().remove(paneNode);
        pane.getChildren().add(zIndex, replaceWith);

    }

    void optionSetTextColor(ContextMenuEvent event, Color color) {
        if (event.getSource() instanceof Pane) {
            Pane pane = (Pane)event.getSource();
            Idea idea = ideaController.getIdeaFromPane(pane);
            Bubble bubble = idea.getBubble();
            bubble.setTextColor(color);

            Text text = ideaController.getTextFromPane(pane);
            text.setFill(color);


        }
    }

    void optionSetTextSize(ContextMenuEvent event, SizeChoice size) {
        Pane pane = ideaController.getPaneFromEvent(event);
        Text text = ideaController.getTextFromPane(pane);

        switch (size) {
            case SMALL: text.setFont(new Font(10));
                break;
            case MEDIUM: text.setFont(new Font(14));
                break;
            case LARGE: text.setFont(new Font(18));
                break;
        }
    }

    void selectAsParent(ContextMenuEvent event) {
        Pane pane = (Pane) event.getSource();
        Idea parent = ideaController.getIdeaFromPane(pane);
        if (parent != manipulatedIdea) {
            // remove child from set of children of the previous parent
            if (manipulatedIdea.hasParent())
                manipulatedIdea.getParent().getChildren().remove(manipulatedIdea);
            parent.addChild(manipulatedIdea);
        }
        contextMenuController.setSelectionState(SelectionState.NONE);
        manipulatedIdea = null;
        ideaController.updateLines(ideaController.getIdeaGroup());
    }


    void selectAsAcquaintance(ContextMenuEvent event) {
        Pane pane = (Pane) event.getSource();
        Idea acquaintance = ideaController.getIdeaFromPane(pane);
        if (acquaintance != manipulatedIdea) {
            addAcquaintance(acquaintance, IdeaConnectionType.EXPLANATION);
        }
        contextMenuController.setSelectionState(SelectionState.NONE);
        manipulatedIdea = null;
        ideaController.updateLines(ideaController.getIdeaGroup());
    }

    private void addAcquaintance(Idea idea, IdeaConnectionType connectionType) {
        ideaController.getIdeas().add(idea);
        idea.addAcquaintance(manipulatedIdea, connectionType);
        ideaController.getLineDrawer().addAcquaintanceLine(idea, manipulatedIdea);
    }

    void removeThisConnection(ContextMenuEvent event) {
        Pane pane = (Pane) event.getSource();
        Idea connectedIdea = ideaController.getIdeaFromPane(pane);
        if (connectedIdea != manipulatedIdea) {
            // check if connection is parent-child
            if (connectedIdea.hasThisChild(manipulatedIdea)) {
                connectedIdea.removeChild(manipulatedIdea);
                ideaController.getLineDrawer().hideLine(manipulatedIdea);
            }
            // check if connection is acquaintance
            if (connectedIdea.hasThisAcquaintance(manipulatedIdea)) {
                connectedIdea.removeAcquaintance(manipulatedIdea);
                ideaController.getLineDrawer().removeAcquaintanceLine(connectedIdea, manipulatedIdea);
            }

            if (manipulatedIdea.hasThisChild(connectedIdea)) {
                manipulatedIdea.removeChild(connectedIdea);
                ideaController.getLineDrawer().hideLine(connectedIdea);
            }

            if (manipulatedIdea.hasThisAcquaintance(connectedIdea)) {
                manipulatedIdea.removeAcquaintance(connectedIdea);
                ideaController.getLineDrawer().removeAcquaintanceLine(manipulatedIdea, connectedIdea);
            }
            // if no connection, do nothing
        }
        contextMenuController.setSelectionState(SelectionState.NONE);
        manipulatedIdea = null;
        ideaController.updateLines(ideaController.getIdeaGroup());
    }
}