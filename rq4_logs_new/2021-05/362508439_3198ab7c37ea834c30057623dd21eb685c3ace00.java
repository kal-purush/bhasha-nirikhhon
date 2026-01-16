package com.ehago.kreamzone.repository;

import com.ehago.kreamzone.entity.Account;
import org.springframework.data.repository.CrudRepository;

public interface AccountRepository extends CrudRepository<Account, Long> {
}