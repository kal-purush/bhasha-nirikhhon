package springProjectFernando.generic;

import java.util.List;

public interface JPAGeneric<T> {

	T findById(Integer id);

	List<T> findAll();

	void save(T t);

	T update(T t);

	void delete(T t);
}