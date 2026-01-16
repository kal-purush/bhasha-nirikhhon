package com.arifacar.service.generic;

import java.util.List;

public interface ICrudService<T> {

    T create(T t);

    T update(T t);

    void delete(T t);

    T findById(Long id);

    List<T> findAll(int id);

}