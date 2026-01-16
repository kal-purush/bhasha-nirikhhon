package pl.dchruscinski.adapter;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import pl.dchruscinski.entity.ProductManager;
import pl.dchruscinski.repository.ProductManagerRepository;

@Repository
public interface AdapterProductManagerRepository extends ProductManagerRepository, JpaRepository<ProductManager, Integer> {

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) > 0 FROM product_manager WHERE id = :managerId")
    boolean existsById(@Param("managerId") Integer managerId);

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM product_manager")
    Integer countProductManagers();
}