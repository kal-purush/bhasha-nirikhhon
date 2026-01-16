package hu.oe.nik.szfmv.automatedcar.model;

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;

import java.util.ArrayList;
import java.util.List;

public class ScriptedPath {
    public enum LoopType {
        NONE, PINGPONG, LOOP
    }

    public enum Direction {
        FORWARDS, BACKWARDS
    }

    private final float deltaTime = 0.04f;
    private final float waypointReachedTreshold = 5f;

    private List<Vector2D> waypoints = new ArrayList<>();
    private LoopType loopType = LoopType.NONE;
    private Direction direction = Direction.FORWARDS;

    private WorldObject worldObject;
    private Vector2D currentPosition;
    private boolean isStopped = false;
    private float movementSpeed = 10f;
    private float rotation = 0f;
    private boolean isRotationSmoothingEnabled;
    private float rotationSpeed;

    private int targetId;
    private Vector2D target;

    public ScriptedPath(WorldObject worldObject) {
        this.worldObject = worldObject;
    }

    public void Start() {
        isStopped = false;
    }

    public void Stop() {
        isStopped = true;
    }

    public void init() {
        currentPosition = new Vector2D(worldObject.getX(), worldObject.getY());
        targetId = 0;
        target = waypoints.get(targetId);
    }

    public void loop() {
        if (!isStopped) {
            moveObjectTowardsWaypoint();
            rotateObjectTowardsWaypoint();
            ApplyPositionAndRotationToObject();
            if (isTargetWaypointReached()) {
                getNewTargetWaypoint();
                if (isPathFinished()) {
                    correctFinalPosition();
                    isStopped = true;
                }
            }
        }
    }

    private void ApplyPositionAndRotationToObject() {
        worldObject.setX((int) currentPosition.getX());
        worldObject.setY((int) currentPosition.getY());
        worldObject.setRotation(rotation);
    }

    private void moveObjectTowardsWaypoint() {
        Vector2D directionVector = target.subtract(currentPosition).normalize();
        Vector2D speedVector = directionVector.scalarMultiply(movementSpeed * deltaTime);
        currentPosition = currentPosition.add(speedVector);
    }

    private void rotateObjectTowardsWaypoint() {
        rotation = (float) Math.atan2(target.getY() - currentPosition.getY(),
            target.getX() - currentPosition.getX());
    }

    private boolean isTargetWaypointReached() {
        return Vector2D.distance(currentPosition, target) <= waypointReachedTreshold;
    }

    private void correctFinalPosition() {
        currentPosition = target;
    }

    private void getNewTargetWaypoint() {
        if (direction == Direction.FORWARDS) {
            getNewForwardWaypointId();
        } else {
            getNewBackwardWaypointId();
        }

        target = waypoints.get(targetId);
    }

    private void getNewForwardWaypointId() {
        if (targetId + 1 < waypoints.size()) {
            targetId += 1;
        } else {
            if (loopType == LoopType.LOOP) {
                targetId = 0;
            }
            if (loopType == LoopType.PINGPONG) {
                targetId -= 1;
                direction = Direction.BACKWARDS;
            }
        }
    }

    private void getNewBackwardWaypointId() {
        if (targetId - 1 >= 0) {
            targetId -= 1;
        } else {
            if (loopType == LoopType.LOOP) {
                targetId = waypoints.size() - 1;
            }
            if (loopType == LoopType.PINGPONG) {
                targetId += 1;
                direction = Direction.FORWARDS;
            }
        }
    }

    private boolean isPathFinished() {
        return loopType == LoopType.NONE && targetId == waypoints.size() - 1;
    }

    public List<Vector2D> getWaypoints() {
        return waypoints;
    }

    public void setWaypoints(List<Vector2D> waypoints) {
        this.waypoints = waypoints;
    }

    public LoopType getLoopType() {
        return loopType;
    }

    public void setLoopType(LoopType loopType) {
        this.loopType = loopType;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public float getMovementSpeed() {
        return movementSpeed;
    }

    public void setMovementSpeed(float movementSpeed) {
        this.movementSpeed = movementSpeed;
    }
}