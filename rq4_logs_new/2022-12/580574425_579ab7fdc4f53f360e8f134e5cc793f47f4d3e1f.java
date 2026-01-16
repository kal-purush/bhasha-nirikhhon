package com.pandora.backend.repository;

import com.pandora.backend.domain.Accounts;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AccountsRepository extends JpaRepository<Accounts, Integer> {
    @Modifying
    @Query("UPDATE accounts acc SET acc.deleted = true WHERE acc.id = ?1")
    void softDelete(Integer id);

    List<Accounts> findAllByDeletedFalse();

    Accounts findByUsername(String username);
}