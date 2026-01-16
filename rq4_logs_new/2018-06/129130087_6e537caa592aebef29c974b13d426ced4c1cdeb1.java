package se.femtearenan.mindmap.utility;

import javafx.geometry.Bounds;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.layout.Pane;
import javafx.scene.shape.Line;
import se.femtearenan.mindmap.model.Idea;
import se.femtearenan.mindmap.ui.IdeaController;

import java.util.HashMap;
import java.util.Map;

public class LineDrawer {

    private IdeaController controller;
    private Scene scene;
    private Map<Idea, Map<Idea, Line>> acquaintanceLineMap = new HashMap<>();

    public LineDrawer(IdeaController controller) {
        this.controller = controller;
        scene = controller.getScene();
    }

    public void addAcquaintanceLines(Idea idea) {
        Map<Idea, Line> lineMap = new HashMap<>();
        for (Idea acquaintance : idea.getAcquaintances().keySet()) {
            Line aLine = new Line();
            lineMap.put(acquaintance, aLine);
            ((Group)scene.getRoot()).getChildren().add(aLine);
        }
        acquaintanceLineMap.put(idea, lineMap);
    }

    public void addAcquaintanceLine(Idea origin, Idea acquaintance) {
        Line line = new Line();
        acquaintanceLineMap.get(origin).put(acquaintance, line);
        controller.addNodeToScene(line);
        drawAcquaintanceLines(origin);

    }

    public Line getAcquaintanceLine(Idea origin, Idea acquaintance) {
        return acquaintanceLineMap.get(origin).get(acquaintance);
    }

    public void removeAcquaintanceLine(Idea origin, Idea remove) {
        acquaintanceLineMap.get(origin).remove(remove);
    }

    public void drawAcquaintanceLines(Idea idea) {
        if (idea.getAcquaintances().size() > 0) {
            Pane start = controller.getIdeaPaneFromGroup(idea.getTheme());

            for (Idea acquaintance : idea.getAcquaintances().keySet()) {

                Pane end = controller.getIdeaPaneFromGroup(acquaintance.getTheme());

                Line line = acquaintanceLineMap.get(idea).get(acquaintance);
                line.getStrokeDashArray().addAll(5.0, 5.0);

                Bounds startBoundsInScene = start.localToScene(start.getBoundsInLocal());
                Bounds endBoundsInScene = end.localToScene(end.getBoundsInLocal());

                // ADJUST START AND END DEPENDING ON WHERE THE SHAPES ARE IN RELATION TO EACH OTHER.
                double startX = startBoundsInScene.getMinX() + startBoundsInScene.getWidth() / 2;
                double startY = startBoundsInScene.getMinY() + startBoundsInScene.getHeight() / 2;
                double endX = endBoundsInScene.getMinX() + endBoundsInScene.getWidth() / 2;
                double endY = endBoundsInScene.getMinY() + endBoundsInScene.getHeight() / 2;

                // START is to the left of END
                if (startBoundsInScene.getMaxX() < endBoundsInScene.getMinX()) {
                    startX = startBoundsInScene.getMaxX();
                    endX = endBoundsInScene.getMinX();
                } else if (startBoundsInScene.getMinX() > endBoundsInScene.getMaxX()) { // START is to the right of END
                    startX = startBoundsInScene.getMinX();
                    endX = endBoundsInScene.getMaxX();
                } else {
                    // START is above the END
                    if (startBoundsInScene.getMaxY() < endBoundsInScene.getMinY()) {
                        startY = startBoundsInScene.getMaxY();
                        endY = endBoundsInScene.getMinY();
                    } else if (startBoundsInScene.getMinY() > endBoundsInScene.getMaxY()) { // START is below the END
                        startY = startBoundsInScene.getMinY();
                        endY = endBoundsInScene.getMaxY();
                    }
                }

                line.setStartX(startX);
                line.setStartY(startY);
                line.setEndX(endX);
                line.setEndY(endY);
            }
        }
    }
}