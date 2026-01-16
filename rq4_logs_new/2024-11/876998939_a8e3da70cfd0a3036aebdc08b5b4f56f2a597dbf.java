package com.smartverse.churchlitebackend.repository.cashtransactions;

import com.smartverse.churchlitebackend_gen.CashTransactionsRepository;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
@Primary
public interface CashTransactionsCustomRepository extends CashTransactionsRepository {

    @Query(nativeQuery = true, value = " select exists (select  * from cash_transactions WHERE cash = ?1) ")
    boolean existsByOpening(String id);
}