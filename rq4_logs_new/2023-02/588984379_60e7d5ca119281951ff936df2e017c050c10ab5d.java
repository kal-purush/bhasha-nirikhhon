package com.springframework.petclinic.services.map;

import com.springframework.petclinic.services.CrudService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractServiceMap<T, ID> implements CrudService<T, ID> {
    private Map<ID,T> map = new HashMap<ID,T>();

    @Override
    public Set<T> findAll(){
        return new HashSet(map.values());
    };

    @Override
    public T findById(ID id){
        return (T)map.get(id);
    }

    @Override
    public T save(ID id, T object){
        map.put(id,object);
        return object;
    }

    @Override
    public void delete(T object){
        map.entrySet().removeIf(entry ->entry.getValue().equals(object));
    }

    @Override
    public void deleteById(ID id){
        map.remove(id);
    }

}