package pl.dchruscinski.adapter;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import pl.dchruscinski.entity.CustomerProductKey;
import pl.dchruscinski.entity.ProductOwnership;
import pl.dchruscinski.repository.ProductOwnershipRepository;

import java.util.Optional;

@Repository
public interface AdapterProductOwnershipRepository extends ProductOwnershipRepository, JpaRepository<ProductOwnership, CustomerProductKey> {

    @Override
    @Query(nativeQuery = true,
            value = "SELECT * FROM product_ownership WHERE customer_id = :customerId AND product_id = :productId")
    Optional<ProductOwnership> findById(@Param("customerId") Integer customerId, @Param("productId") Integer productId);

    @Override
    @Query(nativeQuery = true,
            value = "SELECT COUNT(*) > 0 FROM product_ownership WHERE customer_id = :customerId AND product_id = :productId")
    boolean existsById(@Param("customerId") Integer customerId, @Param("productId") Integer productId);

    @Override
    @Query(nativeQuery = true,
            value = "DELETE FROM product_ownership WHERE customer_id = :customerId AND product_id = :productId")
    void deleteById(@Param("customerId") Integer customerId, @Param("productId") Integer productId);

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM product_ownership")
    int countProductOwnerships();
}