package studio.jawa.bullettrain.factories;

import com.badlogic.ashley.core.Entity;
import com.badlogic.gdx.graphics.Texture;
import studio.jawa.bullettrain.components.gameplays.GeneralStatsComponent;
import studio.jawa.bullettrain.components.gameplays.enemies.EnemyBehaviourComponent;
import studio.jawa.bullettrain.components.gameplays.enemies.EnemyStateComponent;
import studio.jawa.bullettrain.entities.enemies.EnemyEntity;


public class EnemyFactory {
    public  static enum TYPES {
        RANGED,
        MELEE
    }

    public static Entity createEnemy(float x, float y, Texture tex, TYPES type) {
        if (type == TYPES.MELEE) {
            return createMeleeEnemy(x, y, tex);
        }

        return null;
    }

    public static Entity createMeleeEnemy(float x, float y, Texture tex) {
        GeneralStatsComponent enemystat = new GeneralStatsComponent(10, 200);
        EnemyBehaviourComponent behaviour = new EnemyBehaviourComponent(100, 10, 40, false);
        EnemyEntity enemy = new EnemyEntity(300, 300, tex, EnemyStateComponent.STATES.IDLE, behaviour, enemystat);
        return enemy;
    }
}