package io.github.scaliermaelstrom.cardriver;

import android.content.Context;
import android.os.Vibrator;

import java.util.ArrayList;

import io.github.scaliermaelstrom.cardriver.math.Vector2;
import io.github.scaliermaelstrom.cardriver.math.Vector3;
import io.github.scaliermaelstrom.cardriver.rendering.Camera;
import io.github.scaliermaelstrom.cardriver.rendering.Rectangle;
import io.github.scaliermaelstrom.cardriver.rendering.Terrain;
import io.github.scaliermaelstrom.cardriver.rendering.Texture;
import io.github.scaliermaelstrom.cardriver.utils.Collision;

public class Car {

    private final float CAR_HEIGHT = 4;
    private final float CAR_RADIUS = 3;
    private final float TREE_RADIUS = 4;
    private final float SPIKE_RADIUS = 2;
    private final float BOUNDING_SQUARE_SIZE = 50;
    private final float DEFECT_ANGLE = 5;

    private float maxSpeed = 0.75f;
    private float acceleration = 0.03f;

    private boolean defectLeft = false;
    private boolean defectRight = false;

    public Vector3 position;
    private float angle;
    private float wheelAngle = 0;
    private float velocity = 0;
    private Camera camera;
    private long lastHit = 0;

    private Vector2 velVector = new Vector2(0, 0);

    private Vibrator v;

    private Texture viewBase;
    private Texture wheel;

    public Car(Camera camera, Terrain terrain) {
        this.position = new Vector3(
                terrain.startPos.x,
                CAR_HEIGHT,
                terrain.startPos.y
        );
        this.angle = terrain.angle + 90;
        this.camera = camera;

        viewBase = new Texture(R.drawable.view);
        wheel = new Texture(R.drawable.wheel);

        v = (Vibrator) MainActivity.context.getSystemService(Context.VIBRATOR_SERVICE);
    }

    public void update(Vector3 rotation, ArrayList<Vector2> positions, Terrain terrain) {
        boolean onGround = true;

        for (int i = 0; i < terrain.mesh.size() - 2; i++) {
            Vector2 p1 = terrain.mesh.get(i);
            Vector2 p2 = terrain.mesh.get(i + 1);
            Vector2 p3 = terrain.mesh.get(i + 2);

            if (Collision.AABBTriangle(p1, p2, p3, position)) {
                onGround = (Collision.isPointInTriangle(p1, p2, p3, new Vector2(position.x, position.z)));

                if (onGround)
                    break;
            } else {
                onGround = false;
            }
        }

        if (onGround) {
            maxSpeed = terrain.speedOnRoad;
            acceleration = terrain.accelerationOnRoad;
        } else {
            if (Math.abs(velocity) > 0.2)
                v.vibrate(30);
            maxSpeed = terrain.speedOnRoad / 2;
            acceleration = terrain.accelerationOnRoad / 2;
        }

        if (defectLeft || defectRight)
            maxSpeed *= 0.6;

        wheelAngle = Math.max(Math.min(rotation.y * 2, 180), -180);

        boolean start = false;
        boolean stop = false;

        for (Vector2 p : positions) {
            if (p.x < 0f) {
                start = true;
            }

            if (p.x >= 0f) {
                stop = true;
            }
        }

        if (start && !stop)
            velocity += acceleration;
        if (!start && !stop)
            velocity *= terrain.slowingSpeed;
        if (stop) {
            if (velocity > 1) {
                velocity *= terrain.breakSpeed;
            } else {
                velocity -= acceleration / 2;
            }
        }

        velocity = Math.max(Math.min(velocity, maxSpeed), -maxSpeed / 2);

        angle += wheelAngle / 20 * velocity;

        float defectAngle = 0;
        if (defectLeft && !defectRight)
            defectAngle = -DEFECT_ANGLE;
        if (!defectLeft && defectRight)
            defectAngle = DEFECT_ANGLE;

        velVector.x = (float) (velVector.x - Math.cos(Math.toRadians(angle + 90 + defectAngle)) * velocity) / 2;
        velVector.y = (float) (velVector.y - Math.sin(Math.toRadians(angle + 90 + defectAngle)) * velocity) / 2;

        Vector3 newPos = new Vector3(position.x, position.y, position.z);

        Vector2 carPosVec2 = new Vector2(newPos.x, newPos.z);
        Vector2 carPosVec2X = new Vector2(newPos.x + velVector.x, newPos.z);
        Vector2 carPosVec2Y = new Vector2(newPos.x, newPos.z + velVector.y);

        boolean collisionX = false;
        boolean collisionY = false;
        for (Vector2 vec : terrain.trees) {
            if (Collision.AABB(carPosVec2, BOUNDING_SQUARE_SIZE, BOUNDING_SQUARE_SIZE, vec)) {
                // Tree is inside bounding square
                if (!collisionX && Collision.cylinderCollision(vec, TREE_RADIUS, carPosVec2X, CAR_RADIUS))
                    collisionX = true;
                if (!collisionY && Collision.cylinderCollision(vec, TREE_RADIUS, carPosVec2Y, CAR_RADIUS))
                    collisionY = true;
            }
        }
        if (System.currentTimeMillis() - lastHit > 300) {
            lastHit = System.currentTimeMillis();
            for (Vector2 vec : terrain.spikes) {
                if (Collision.AABB(carPosVec2, BOUNDING_SQUARE_SIZE, BOUNDING_SQUARE_SIZE, vec)) {
                    // Spike is inside bounding square
                    if (Collision.cylinderCollision(vec, SPIKE_RADIUS, carPosVec2, CAR_RADIUS)) {
                        // Hit spike
                        if (Math.random() < 0.5) {
                            defectLeft = true;
                        } else {
                            defectRight = true;
                        }
                        v.vibrate(100);
                    }
                }
            }
        }

        if (!collisionX)
            newPos.x += velVector.x;
        if (!collisionY)
            newPos.z += velVector.y;

        newPos.x = Math.max(Math.min(newPos.x, terrain.SIZE / 2 - 1), -terrain.SIZE / 2 + 1);
        newPos.z = Math.max(Math.min(newPos.z, terrain.SIZE / 2 - 1), -terrain.SIZE / 2 + 1);

        newPos.y = CAR_HEIGHT + (float) (onGround ? 0 : Math.random() * velocity - velocity / 2);
        position = newPos;

        if (collisionX || collisionY)
            velocity = 0;

        camera.setPosition(position);
        camera.setAngle(angle);
    }

    public void draw() {
        Rectangle.draw(viewBase, new Vector3(0, 0, 0), new Vector2(camera.size.x, camera.size.x), camera, Rectangle.DrawType.UI);
        Rectangle.draw(wheel, new Vector3(-0.2f, -0.25f, 1f), new Vector2(1, 1), wheelAngle, camera, Rectangle.DrawType.UI);
    }

}