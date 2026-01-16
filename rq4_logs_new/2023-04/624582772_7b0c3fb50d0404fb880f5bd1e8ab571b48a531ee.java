package pl.dchruscinski.adapter;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import pl.dchruscinski.entity.Product;
import pl.dchruscinski.repository.ProductRepository;

@Repository
public interface AdapterProductRepository extends ProductRepository, JpaRepository<Product, Integer> {

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) > 0 FROM product WHERE id = :id")
    boolean existsById(@Param("id") Integer id);

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM product")
    int countProducts();
}