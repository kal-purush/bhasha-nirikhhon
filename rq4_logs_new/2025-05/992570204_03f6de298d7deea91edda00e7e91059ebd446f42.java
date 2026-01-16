package studio.jawa.bullettrain.components.gameplays.enemies;

import com.badlogic.ashley.core.Component;

public class EnemyBehaviourComponent implements Component {
    public float aggroRange;
    public float attackRange;
    public float coolDown;
    public boolean preferDistance;

    public EnemyBehaviourComponent(float aggroRange, float attackRange, float coolDown, boolean preferDistance) {
        this.aggroRange = aggroRange;
        this.attackRange = attackRange;
        this.coolDown = coolDown;
        this.preferDistance = preferDistance;
    }
}