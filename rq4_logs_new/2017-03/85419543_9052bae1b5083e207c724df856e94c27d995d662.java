package com.holyribbas.dao;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import com.holyribbas.model.IAbstractEntity;

public abstract class AbstractDao {

	public EntityManager manager;

	public AbstractDao(EntityManager manager) {
		this.manager = manager;
	}

	public void inserir(IAbstractEntity aluno) {
		manager.persist(aluno);
	}

	public void atualizar(IAbstractEntity entity) {
		manager.merge(entity);
	}

	public void excluir(IAbstractEntity entity) {
		//entity = manager.find(entityClass(), entity.getId());
		manager.remove(entity);
	}

	public abstract Class<IAbstractEntity> entityClass();

	public IAbstractEntity buscarPorId(Long id) {
		return manager.find(entityClass(), id);
	}

	@SuppressWarnings("unchecked")
	public List<IAbstractEntity> listar() {
		Query query = manager.createQuery("select c from " + entityClass().getSimpleName() + " c");
		return query.getResultList();
	}

}