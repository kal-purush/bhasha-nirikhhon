package studio.jawa.bullettrain.entities.testing;

import com.badlogic.ashley.core.Entity;
import com.badlogic.gdx.graphics.Texture;
import com.badlogic.gdx.graphics.g2d.Sprite;
import studio.jawa.bullettrain.components.technicals.SpriteComponent;
import studio.jawa.bullettrain.components.technicals.TransformComponent;

public class TestingDummy extends Entity {
    public TestingDummy(float x, float y, Texture tex) {
        add(new TransformComponent(x, y));
        add(new SpriteComponent(new Sprite(tex)));
    }
}