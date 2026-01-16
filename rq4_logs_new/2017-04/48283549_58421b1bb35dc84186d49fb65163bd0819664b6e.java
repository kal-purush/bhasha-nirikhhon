package com.agg.application.dao;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Component;

import com.agg.application.entity.Check;

@Component
public interface CheckDAO extends CrudRepository<Check, Long> {

	
}