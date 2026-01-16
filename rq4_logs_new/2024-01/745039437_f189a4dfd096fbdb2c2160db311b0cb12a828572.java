package com.transaction.management.DAO;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.transaction.management.entity.Transaction;

@Transactional
public interface TransactionDAO  extends JpaRepository<Transaction, Long> {
	
	@Query("select t from Transaction t where t.transactionDate=?1")
	public List<Transaction> transactionsOnDate(Date date);
	
	@Query("select t from Transaction t where t.transactionDate in (?1,?2)")
	public List<Transaction> transactionsBetweenDate(Date date1,Date date2);

}