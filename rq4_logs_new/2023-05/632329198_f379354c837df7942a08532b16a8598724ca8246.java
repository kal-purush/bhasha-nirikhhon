package ru.wasabi.my_atm.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import ru.wasabi.my_atm.entity.transaction.OperationList;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OperationListRepository extends JpaRepository<OperationList, Long> {

    List<OperationList> findByAccountIdOrderByCreatedAtDesc(Long accountId);

    @Query(value = "SELECT * FROM operation_list WHERE account_id = :accountId AND DATE_TRUNC('day', created_at) BETWEEN :startDate AND :endDate ORDER BY DATE_TRUNC('day', created_at) DESC", nativeQuery = true)
    List<OperationList> findByAccountIdAndCreatedAtBetween(Long accountId, LocalDate startDate, LocalDate endDate);
}