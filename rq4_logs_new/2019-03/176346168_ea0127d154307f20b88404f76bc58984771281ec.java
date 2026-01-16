package util;

import java.util.ArrayList;
import java.util.Collection;

public class EntityManager {
	private static ArrayList<Entity> entités;
	
	public static boolean add(Entity e) {
		return entités.add(e);
	}
	
	public static boolean addAll(Collection<Entity> c) {
		return entités.addAll(c);
	}
	
	public static boolean remove(Entity e) {
		return entités.remove(e);
	}
	
	public static boolean removeAll(Collection<Entity> c) {
		return entités.removeAll(c);
	}
	public static void updateAll() {
		for (Entity entity : entités) {
			entity.update();
		}
	}
}