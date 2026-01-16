package uqbar.arena.persistence;

import java.util.List;

import org.uqbar.commons.model.Entity;
import org.uqbar.commons.model.Home;

public abstract class PersistentHome<T extends Entity> implements Home<T> {

	private PersistentHomeImpl<T> homeImpl = new PersistentHomeImpl<T>();

	@Override
	public T searchById(int id) {
		return homeImpl.searchById(id, this);
	}

	@Override
	public List<T> searchByExample(T example) {
		return homeImpl.searchByExample(example);
	}

	@Override
	public List<T> allInstances() {
		return homeImpl.allInstances(this);
	}

	@Override
	public void create(T object) {
		homeImpl.create(object);
	}

	@Override
	public void update(T object) {
		homeImpl.update(object);
	}

	@Override
	public void delete(T object) {
		homeImpl.delete(object);
	}
}