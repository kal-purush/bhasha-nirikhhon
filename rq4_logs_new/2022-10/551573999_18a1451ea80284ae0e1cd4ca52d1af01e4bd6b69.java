package com.clone.trello.dao;

import java.util.List;

public interface BaseDAO<T> {
	public List<T> findRecordsEqualToValue(String key, String value);

	public List<T> findRecordsGreaterThanValue(String key, Long value);
}