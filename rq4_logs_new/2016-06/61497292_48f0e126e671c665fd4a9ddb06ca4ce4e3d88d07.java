package by.training.filmstore.dao;

import java.io.Serializable;
import java.util.List;

import by.training.filmstore.dao.exception.FilmStoreDAOException;

public interface AbstractDAO <T extends Serializable,K extends Serializable>{
	 T find(K id) throws FilmStoreDAOException;
	 List<T> findAll() throws FilmStoreDAOException;
	 boolean create(T entity) throws FilmStoreDAOException;
	 boolean update(T entity) throws FilmStoreDAOException;
	 boolean delete(K id) throws FilmStoreDAOException;
}