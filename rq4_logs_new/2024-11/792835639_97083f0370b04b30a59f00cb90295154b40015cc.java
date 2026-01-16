package com.jinius.ecommerce.order.infra;

import com.jinius.ecommerce.order.domain.OrderItemStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface OrderItemJpaRepository extends JpaRepository<OrderItemEntity, Long> {
    @Query(value =
            "SELECT " +
                "oi.id AS orderItemId " +
            "FROM OrderItemEntity oi " +
            "JOIN ProductEntity p ON p.id = oi.productId " +
            "JOIN OrderEntity o ON o.id = oi.orderId " +
            "WHERE oi.status = :status " +
            "AND o.status = 'COMPLETED'")
    List<Long> findPreparingItems(@Param("status") OrderItemStatus status);

    @Modifying
    @Transactional
    @Query(value = "UPDATE OrderItemEntity " +
            "SET status = :status " +
            "WHERE id IN (:ids)")
    void updateStatus(@Param("ids") List<Long> ids, @Param("status") OrderItemStatus status);
}