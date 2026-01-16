package carleton.sysc4907.controller.element;

import carleton.sysc4907.EditingAreaProvider;
import carleton.sysc4907.processing.ElementCreator;
import carleton.sysc4907.processing.ElementIdManager;
import carleton.sysc4907.view.DiagramElement;
import javafx.scene.layout.Pane;

public class ConnectorMovePointPreviewCreator {

    private final ElementIdManager elementIdManager;

    private final ElementCreator elementCreator;

    public ConnectorMovePointPreviewCreator(ElementIdManager elementIdManager, ElementCreator elementCreator) {
        this.elementIdManager = elementIdManager;
        this.elementCreator = elementCreator;
    }

    public ConnectorElementController createMovePointPreview(ConnectorElementController controller) {
        // Create a new connector element and get its controller
        var element = elementCreator.create("Line", elementIdManager.getNewId(), true);
        var previewController = (ConnectorElementController) element.getProperties().get("controller");
        element.setOpacity(0.3);
        previewController.setPathingStrategy(controller.getPathingStrategy());
        previewController.setStartX(controller.getStartX());
        previewController.setEndX(controller.getEndX());
        previewController.setStartY(controller.getStartY());
        previewController.setEndY(controller.getEndY());
        previewController.setIsStartHorizontal(controller.getIsStartHorizontal());
        previewController.setIsEndHorizontal(controller.getIsEndHorizontal());
        previewController.setSnapStart(controller.getSnapStart());
        previewController.setSnapEnd(controller.getSnapEnd());

        Pane editingArea = EditingAreaProvider.getEditingArea();
        editingArea.getChildren().add(element);

        return previewController;
    }

    public void deleteMovePreview(DiagramElement element, ConnectorElementController connectorElementController) {
        if (connectorElementController == null) {
            return;
        }
        ((Pane) element.getParent()).getChildren().remove(connectorElementController.element);
    }
}