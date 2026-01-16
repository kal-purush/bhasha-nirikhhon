package com.kodilla.ecommercee;

import com.kodilla.ecommercee.domain.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Transactional
@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

    @Override
    Product save(Product product);

    @Override
    List<Product> findAll();

    @Override
    Optional<Product> findById(Long id);

    @Override
    void deleteById(Long id);
}