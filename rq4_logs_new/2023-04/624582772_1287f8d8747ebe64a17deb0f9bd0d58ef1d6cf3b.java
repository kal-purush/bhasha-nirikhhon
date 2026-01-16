package pl.dchruscinski.adapter;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import pl.dchruscinski.entity.ProductPurchase;
import pl.dchruscinski.repository.ProductPurchaseRepository;

import java.util.List;

@Repository
public interface AdapterProductPurchaseRepository extends ProductPurchaseRepository, JpaRepository<ProductPurchase, Integer> {

    @Override
    @Query(nativeQuery = true,
            value = "SELECT COUNT(*) > 0 FROM product_purchase WHERE id = :purchaseId")
    boolean existsById(@Param("purchaseId") Integer purchaseId);

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM product_purchase")
    Integer countProductPurchases();

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM product_purchase WHERE customer_id = :customerId")
    Integer countProductPurchasesByCustomer(@Param("customerId") Integer customerId);

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM product_purchase WHERE product_id = :productId")
    Integer countProductPurchasesByProduct(@Param("productId") Integer productId);

    @Override
    @Query(nativeQuery = true, value = "SELECT * FROM product_purchase WHERE customer_id = :customerId")
    List<ProductPurchase> findAllByCustomer(@Param("customerId") Integer customerId);

    @Override
    @Query(nativeQuery = true, value = "SELECT * FROM product_purchase WHERE product_Id = :productId")
    List<ProductPurchase> findAllByProduct(@Param("productId") Integer productId);
}