package com.fra.clientes.dao;

import java.sql.SQLException;
import java.util.List;

public interface Dao<T> {

	public void add(T c);

	public void update(T c) throws SQLException;

	public void deleteById(long id) throws SQLException;

	public T getById(long id) throws SQLException;

	public List<T> get();
}