package org.ctp.crashapi.nms.damage;

import java.lang.reflect.Method;

import org.bukkit.entity.Arrow;
import org.bukkit.entity.LivingEntity;
import org.ctp.crashapi.nms.NMS;

import net.minecraft.util.MathHelper;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EntityLiving;
import net.minecraft.world.entity.projectile.EntityArrow;
import net.minecraft.world.phys.Vec3D;

public class Damage_3 extends NMS {

	public static double getArrowDamage(LivingEntity le, Arrow a) {
		EntityArrow arrow = (EntityArrow) getCraftBukkitEntity(a);
		EntityLiving entity = (EntityLiving) getCraftBukkitEntity(le);
		float f = 0;
		try {
			Class<?> c = Entity.class;
			Method m = c.getDeclaredMethod("de");
			Object o = m.invoke(arrow);
			if (o instanceof Vec3D) f = (float) ((Vec3D) o).f();
			Class<?> cArrow = EntityArrow.class;
			Method dm = cArrow.getDeclaredMethod("n");
			Object dmo = dm.invoke(arrow);
			if (dmo instanceof Number) {
				int i = MathHelper.e(Math.max(f * (Integer) dmo, 0.0D));
				arrow.a(entity, i);
				return (int) dm.invoke(arrow) / 2;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return 0;
	}

	public static void updateHealth(LivingEntity le) {
		Entity entity = getCraftBukkitEntity(le);
		if (entity instanceof EntityLiving) {
			EntityLiving living = (EntityLiving) entity;
			try {
				Class<?> c = EntityLiving.class;
				Method m1 = c.getMethod("ea");
				Object o1 = m1.invoke(living);
				Method m2 = c.getMethod("c", float.class);
				if (o1 instanceof Number) m2.invoke(living, (float) o1);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}