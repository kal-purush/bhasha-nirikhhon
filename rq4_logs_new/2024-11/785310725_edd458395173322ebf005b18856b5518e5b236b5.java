package org.dungeon.prototype.model.level.generation;

import lombok.Data;
import org.dungeon.prototype.model.Point;
import org.dungeon.prototype.model.level.ui.GridSection;
import org.dungeon.prototype.model.weight.Weight;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.math3.util.FastMath.abs;

@Data
public class LevelGridCluster {
    Point startConnectionPoint;
    Point endConnectionPoint;
    int size = 0;
    int negativeRoomsCount = 0;
    List<GridSection> deadEnds = new ArrayList<>();
    Weight clusterExpectedWeight;

    public LevelGridCluster(Point startConnectionPoint, Point endConnectionPoint) {
        this.startConnectionPoint = startConnectionPoint;
        this.endConnectionPoint = endConnectionPoint;
    }

    public void incrementSize() {
        size++;
    }

    public double getDensity() {
        return size /
                ((double) abs(endConnectionPoint.getX() - startConnectionPoint.getX()) *
                        abs(endConnectionPoint.getY() - startConnectionPoint.getY()));
    }

    public boolean hasSmallSide() {
        return (abs(endConnectionPoint.getX() - startConnectionPoint.getX()) < 4) ||
                (abs(endConnectionPoint.getY() - startConnectionPoint.getY()) < 4);
    }

    public boolean isSmallCluster() {
        return (abs(endConnectionPoint.getX() - startConnectionPoint.getX()) < 4) &&
                (abs(endConnectionPoint.getY() - startConnectionPoint .getY()) < 4);
    }

    public boolean hasNegativeRooms() {
        return negativeRoomsCount > 0;
    }

    public void incrementNegativeRoomsCount() {
        negativeRoomsCount++;
    }

    public void decrementNegativeRoomsCount() {
        negativeRoomsCount--;
    }

    public boolean hasDeadEnds() {
        return deadEnds.size() > 0;
    }

    public void addDeadEnd(GridSection section) {
        deadEnds.add(section);
    }

    public void removeDeadEnd(GridSection deadEnd) {
        deadEnds.remove(deadEnd);
    }

    public void addDeadEnds(List<GridSection> processedDeadEnds) {
        deadEnds.addAll(processedDeadEnds);
    }
}