package studio.jawa.bullettrain.systems.gameplays.enemies;

import com.badlogic.ashley.core.*;
import com.badlogic.ashley.utils.ImmutableArray;
import com.badlogic.gdx.math.MathUtils;
import com.badlogic.gdx.math.Vector2;
import studio.jawa.bullettrain.components.gameplays.GeneralStatsComponent;
import studio.jawa.bullettrain.components.gameplays.enemies.*;
import studio.jawa.bullettrain.components.technicals.InputComponent;
import studio.jawa.bullettrain.components.technicals.PlayerControlledComponent;
import studio.jawa.bullettrain.components.technicals.TransformComponent;
import studio.jawa.bullettrain.components.technicals.VelocityComponent;

public class EnemyStrafeSystem extends EntitySystem {
    private final ComponentMapper<EnemyStateComponent> sm = ComponentMapper.getFor(EnemyStateComponent.class);
    private final ComponentMapper<TransformComponent> tm = ComponentMapper.getFor(TransformComponent.class);
    private final ComponentMapper<EnemyBehaviourComponent> bm = ComponentMapper.getFor(EnemyBehaviourComponent.class);
    private final ComponentMapper<EnemyStrafeComponent> stm = ComponentMapper.getFor(EnemyStrafeComponent.class);
    private final ComponentMapper<VelocityComponent> vm = ComponentMapper.getFor(VelocityComponent.class);
    private final ComponentMapper<GeneralStatsComponent> gsm = ComponentMapper.getFor(GeneralStatsComponent.class);

    private Family playerFamily = Family.all(TransformComponent.class, PlayerControlledComponent.class).get();

    private ImmutableArray<Entity> entities;
    public EnemyStrafeSystem(Engine engine) {
        this.entities = engine.getEntitiesFor(Family.all(EnemyComponent.class, EnemyStateComponent.class, EnemyBehaviourComponent.class).get());
    }

    @Override
    public void update(float deltaTime) {
        for (Entity entity : entities) {

            EnemyStateComponent state = sm.get(entity);
            if (state.state != EnemyStateComponent.STATES.STRAFE) return; // if not strafing, return

            // get reference to player
            ImmutableArray<Entity> players = getEngine().getEntitiesFor(playerFamily);
            Entity player = players.first();

            // check for player existance
            if (player == null) {
                state.state = EnemyStateComponent.STATES.IDLE; // if no player then just return
                return;
            }

            // enemy info
            EnemyStrafeComponent strafe = stm.get(entity);
            VelocityComponent vel = vm.get(entity);
            GeneralStatsComponent stat = gsm.get(entity);

            // Get important datas
            TransformComponent transform = tm.get(entity);
            EnemyBehaviourComponent behaviour = bm.get(entity);
            TransformComponent playerTransform = player.getComponent(TransformComponent.class);


           if (strafe.attack) {
               System.out.println("BANG");
               strafe.attack = false;
           }

           Vector2 direction = moveLogic(deltaTime, playerTransform, transform, strafe, behaviour);

           direction = direction.scl(stat.speed);

           vel.velocity.set(direction);

           strafe.duration -= deltaTime;

           if (strafe.duration < 0) {
               entity.remove(EnemyStrafeComponent.class);
               entity.add(new EnemyStrafeComponent(behaviour.coolDown));
           }

        }

    }

    protected Vector2 moveLogic(float deltaTime, TransformComponent playerTransform, TransformComponent enemyTransform, EnemyStrafeComponent strafe, EnemyBehaviourComponent behaviour) {
        // Vector from player to enemy
        Vector2 toEnemy = new Vector2(enemyTransform.position).sub(playerTransform.position);

        // Distance to maintain
        float targetDistance = behaviour.aggroRange;

        // Step 1: Direction from player to enemy (for distance checking)
        float currentDistance = toEnemy.len();
        toEnemy.nor();

        // Step 2: Get perpendicular direction for strafing
        Vector2 strafeDir = new Vector2(toEnemy);
        if (strafe.clockWise) {
            strafeDir.rotateDeg(-90);
        } else {
            strafeDir.rotateDeg(90);
        }

        // Step 3: Add small randomness to the strafe direction (±15 degrees)
        float randomOffset = strafe.offset;
        strafeDir.rotateDeg(randomOffset).nor();

        // Step 4: Combine strafe + maintain distance logic
        Vector2 moveDir = new Vector2();

        // If too close to player, also add a little away movement
        if (currentDistance < targetDistance) {
            moveDir.add(toEnemy); // move away
        }
        // If too far, move slightly toward the player
        else if (currentDistance > targetDistance + 30f) {
            moveDir.sub(toEnemy); // move toward
        }

        moveDir.add(strafeDir).nor(); // Combine with strafe


       return new Vector2(moveDir);
    }
}