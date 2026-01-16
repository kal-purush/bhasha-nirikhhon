package pl.dchruscinski.adapter;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import pl.dchruscinski.entity.ProductCategory;
import pl.dchruscinski.repository.ProductCategoryRepository;

@Repository
public interface AdapterProductCategoryRepository extends ProductCategoryRepository, JpaRepository<ProductCategory, Integer> {

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) > 0 FROM product_category WHERE id = :categoryId")
    boolean existsById(@Param("categoryId") Integer categoryId);

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM product_category")
    Integer countProductCategories();
}