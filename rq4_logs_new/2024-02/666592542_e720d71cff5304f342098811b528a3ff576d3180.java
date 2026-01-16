package com.opalsmile.fnc.entity.goals;

import com.opalsmile.fnc.entity.Sabertooth;
import net.minecraft.world.entity.PathfinderMob;
import net.minecraft.world.entity.ai.goal.AvoidEntityGoal;
import net.minecraft.world.entity.animal.Wolf;
import net.minecraft.world.entity.monster.Creeper;

public class SabertoothFearEventListener {

    public static void onEntitySpawn(final PathfinderMob mob) {
        if (mob instanceof Creeper || mob instanceof Wolf) {
            mob.goalSelector.addGoal(3, new AvoidEntityGoal<>(mob, Sabertooth.class, 6.0f, 1.0, 1.2));
        }
    }
}