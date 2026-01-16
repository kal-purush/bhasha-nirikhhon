package com.makebono.algorithms.pathfinder.astaralgorithm;

import java.util.LinkedList;
import java.util.PriorityQueue;

import com.makebono.algorithms.pathfinder.astaralgorithm.tools.GComparator;

/** 
 * @ClassName: Node 
 * @Description: Node holds location infos. 
 * @author makebono
 * @date 2017年11月1日 上午9:12:43 
 *  
 */
public class AStarNode {
    private int id;
    private double x;
    private double y;
    private final LinkedList<AStarNode> children;
    private double gScore;
    private double fScore;
    private final PriorityQueue<AStarNode> parent;

    public AStarNode() {
        final GComparator gc = new GComparator();
        this.id = 0;
        this.x = 0;
        this.y = 0;
        this.children = new LinkedList<AStarNode>();
        this.gScore = Double.POSITIVE_INFINITY;
        this.fScore = Double.POSITIVE_INFINITY;
        this.parent = new PriorityQueue<AStarNode>(gc);
    }

    public AStarNode(final int id, final double x, final double y) {
        final GComparator gc = new GComparator();
        this.id = id;
        this.x = x;
        this.y = y;
        this.children = new LinkedList<AStarNode>();
        this.gScore = Double.POSITIVE_INFINITY;
        this.fScore = Double.POSITIVE_INFINITY;
        this.parent = new PriorityQueue<AStarNode>(gc);
    }

    public void addParent(final AStarNode parent) {
        this.parent.add(parent);
    }

    public void setId(final int id) {
        this.id = id;
    }

    public void setX(final double x) {
        this.x = x;
    }

    public void setY(final double y) {
        this.y = y;
    }

    public void setFScore(final double fScore) {
        this.fScore = fScore;
    }

    public void addChild(final AStarNode node) {
        this.children.add(node);
    }

    public void removeChild(final AStarNode node) {
        this.children.remove(node);
    }

    public void setGScore(final double gScore) {
        this.gScore = gScore;
    }

    public double getFScore() {
        return this.fScore;
    }

    public int getId() {
        return this.id;
    }

    public double getX() {
        return this.x;
    }

    public double getY() {
        return this.y;
    }

    public PriorityQueue<AStarNode> getParent() {
        return this.parent;
    }

    public LinkedList<AStarNode> getChildren() {
        return this.children;
    }

    public double getEuclideangScore(final AStarNode node) {
        return Math.sqrt(Math.pow(this.getX() - node.getX(), 2) + Math.pow(this.getY() - node.getY(), 2));
    }

    public double getGScore() {
        return this.gScore;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("This is node");
        sb.append(this.getId());
        sb.append("\nInfomation about this node: \n    X is: ");
        sb.append(this.getX());
        sb.append("\n    Y is: ");
        sb.append(this.getY());
        sb.append("\n\nIt has child(ren): \n");
        for (int i = 0; i < this.getChildren().size(); i++) {
            sb.append("    node");
            sb.append(this.getChildren().get(i).getId());
            sb.append("\n");
        }
        return sb.toString();
    }
}