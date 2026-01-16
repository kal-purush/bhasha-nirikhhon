package carleton.sysc4907.controller.element;

import carleton.sysc4907.command.ConnectorMovePointCommandFactory;
import carleton.sysc4907.command.MoveCommandFactory;
import carleton.sysc4907.controller.element.arrows.Arrowhead;
import carleton.sysc4907.controller.element.arrows.ArrowheadFactory;
import carleton.sysc4907.controller.element.arrows.ArrowheadType;
import carleton.sysc4907.controller.element.pathing.PathingStrategy;
import carleton.sysc4907.model.DiagramModel;
import javafx.beans.InvalidationListener;
import javafx.beans.Observable;
import javafx.fxml.FXML;
import javafx.scene.shape.Path;

/**
 * Controller for a connector element with an arrow on the end point.
 */
public class ArrowConnectorElementController extends ConnectorElementController {

    private final ArrowheadFactory arrowheadFactory;

    private Arrowhead arrowhead;

    @FXML
    private Path arrowheadPath;

    /**
     * Constructs a new ArrowConnectorElementController.
     * @param previewCreator the move preview creator to use
     * @param moveCommandFactory the factory for making move commands
     * @param diagramModel the active diagram's diagram model
     * @param connectorHandleCreator the connector handle creator to use
     * @param connectorMovePointPreviewCreator the connector preview creator to use
     * @param connectorMovePointCommandFactory the factory for making move point commands
     * @param pathingStrategy the pathing strategy to use for the connector's pathing
     * @param arrowheadFactory the factory for making arrowheads
     */
    public ArrowConnectorElementController(
            MovePreviewCreator previewCreator,
            MoveCommandFactory moveCommandFactory,
            DiagramModel diagramModel,
            ConnectorHandleCreator connectorHandleCreator,
            ConnectorMovePointPreviewCreator connectorMovePointPreviewCreator,
            ConnectorMovePointCommandFactory connectorMovePointCommandFactory,
            PathingStrategy pathingStrategy,
            ArrowheadFactory arrowheadFactory) {
        super(
                previewCreator,
                moveCommandFactory,
                diagramModel,
                connectorHandleCreator,
                connectorMovePointPreviewCreator,
                connectorMovePointCommandFactory,
                pathingStrategy
        );
        this.arrowheadFactory = arrowheadFactory;

        // When most things change, the arrowhead needs to be redrawn.
        InvalidationListener listener = observable -> makeArrowheadPath();
        startX.addListener(listener); // start point affects orientation for direct paths
        startY.addListener(listener);
        endX.addListener(listener); // end point affects where the arrow is drawn
        endY.addListener(listener);
        isEndHorizontal.addListener(listener); // direction affects arrow direction
        this.pathingStrategy.addListener(listener); // pathing strategy could be direct or horizontal/vertical only
    }

    /**
     * Sets the arrowhead type to use for this connector.
     * @param type the new ArrowheadType to use for this connector
     */
    public void setArrowheadType(ArrowheadType type) {
        arrowhead = arrowheadFactory.createArrowhead(type);
        makeArrowheadPath();
    }

    @Override
    protected void recalculatePath() {
        super.recalculatePath();
        makeArrowheadPath();
    }

    /**
     * Draws the arrowhead, if one has been set.
     */
    private void makeArrowheadPath() {
        if (arrowhead != null) {
            arrowhead.makeArrowheadPath(
                    arrowheadPath,
                    adjustX(getStartX()), adjustY(getStartY()),
                    adjustX(getEndX()), adjustY(getEndY()),
                    isEndHorizontal.get(), this.pathingStrategy.get().isDirectPath()
            );
        }
    }

    @Override
    public void initialize() {
        super.initialize();
        setArrowheadType(ArrowheadType.ASSOCIATION); // default arrowhead
    }
}