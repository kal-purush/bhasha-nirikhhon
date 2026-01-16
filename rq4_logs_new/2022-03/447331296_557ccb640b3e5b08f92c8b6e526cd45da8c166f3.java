package LayoutManagement.GraphDrawing.View;

import LayoutManagement.Math.Vector2D;
import model.Edge;
import model.MetaGraph.ExpGraph;
import model.interfaces.iGraph;

import java.awt.*;

import static LayoutManagement.VisualEditor.LayoutConfigs.NODE_SIZE;

public class ExpEdgeComponent {

    private Vector2D posA;
    private Vector2D posB;

    public ExpEdgeComponent(iGraph graph, Edge edge) {
        posA = graph.getNode(edge.getNodeA()).getPos();
        posB = graph.getNode(edge.getNodeB()).getPos();
    }

    public ExpEdgeComponent(Vector2D posA, Vector2D posB) {
        this.posA = posA;
        this.posB = posB;
    }

    public void drawEdge(Graphics2D g2d) {
        g2d.setColor(Color.GRAY);
        g2d.drawLine((int)(posA.getScreenTransX()+(NODE_SIZE*0.5)),
                (int)(posA.getScreenTransY()+(NODE_SIZE*0.5)),
                (int)(posB.getScreenTransX()+(NODE_SIZE*0.5)),
                (int)(posB.getScreenTransY()+(NODE_SIZE*0.5)));
    }

}