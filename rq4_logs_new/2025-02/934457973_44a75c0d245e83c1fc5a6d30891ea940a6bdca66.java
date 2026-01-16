package com.proyectofinal.clave_compas.bd.clavecompas.repositories;

import com.proyectofinal.clave_compas.bd.clavecompas.entities.ProductEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;


public interface ProductRepository extends JpaRepository<ProductEntity, Integer> {

    public Page<ProductEntity> findAll(Pageable pageable);
    public Optional<ProductEntity> findByName(String name);
}