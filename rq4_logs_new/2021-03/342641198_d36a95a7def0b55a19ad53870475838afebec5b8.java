package com.banking.bankservice.repository;

import com.banking.bankservice.model.Account;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface AccountRepository extends JpaRepository<Account, Long> {
    @Query("SELECT a FROM accounts a JOIN FETCH a.user WHERE a.user.phoneNumber = :phone")
    List<Account> findAllAccountsByPhoneNumber(String phone);

    Optional<Account> getAccountByAccountNumber(String number);
}