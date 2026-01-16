package controller;

import javafx.geometry.Point2D;
import javafx.scene.Scene;
import model.Idea;
import model.PointSer;

import java.io.Serializable;
import java.util.*;

public class IdeaTracker implements Serializable{
    private static final long serialVersionUID = 1L;
    Map<Idea, PointSer> paneSceneTrack;

    transient Scene scene;
    transient IdeaController controller;

    public IdeaTracker(IdeaController controller) {
        this.paneSceneTrack = new HashMap<>();

        this.scene = controller.getScene();
        this.controller = controller;
    }

    public void update() {
        paneSceneTrack.clear();
        populateMap();
    }

    private void populateMap() {
        Set<Idea> ideas = controller.getAllIdeas();

        for (Idea idea : ideas) {
            addIdeaToMap(idea);
        }

    }

    private void addIdeaToMap(Idea idea) {
        Point2D point = controller.getScenePointFromIdea(idea);
        PointSer pointSer = new PointSer(point.getX(), point.getY());

        paneSceneTrack.put(idea, pointSer);
    }

    public Map<Idea, PointSer> getPaneSceneTrack() {
        update();
        return paneSceneTrack;
    }


}