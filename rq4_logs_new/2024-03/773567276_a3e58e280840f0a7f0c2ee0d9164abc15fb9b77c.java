package com.albydaa.equinefarm.base;


import jakarta.persistence.MappedSuperclass;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Optional;

@MappedSuperclass
public class BaseServiceImpl<T extends BaseEntity> implements BaseService<T> {
    @Autowired
    private BaseRepo<T> baseRepo;

    @Override
    public Optional<T> findObjectById(long id) {
        return baseRepo.findById(id);
    }

    @Override
    public List<T> findAllObjects() {
        return baseRepo.findAll();
    }

    @Override
    public T saveObject(T object) {
        return baseRepo.save(object);
    }
    @Override
    public T updateExistingObject(T object) {
        return baseRepo.save(object);
    }
    @Override

    public void deleteObjectById(long id) {
        baseRepo.deleteById(id);
    }
}