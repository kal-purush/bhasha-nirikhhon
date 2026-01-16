package studio.jawa.bullettrain.entities.players;

import com.badlogic.ashley.core.Engine;
import com.badlogic.ashley.core.Entity;
import com.badlogic.gdx.graphics.Texture;
import com.badlogic.gdx.graphics.g2d.Sprite;
import studio.jawa.bullettrain.components.technicals.*;
import studio.jawa.bullettrain.systems.technicals.InputMovementSystem;

public class PlayerEntity extends Entity {
    public PlayerEntity(float x, float y, Texture tex) {
        add(new TransformComponent(x, y));
        add(new SpriteComponent(new Sprite(tex)));
        add(new VelocityComponent());
        add(new PlayerControlledComponent());
        add(new InputComponent());
    }
}