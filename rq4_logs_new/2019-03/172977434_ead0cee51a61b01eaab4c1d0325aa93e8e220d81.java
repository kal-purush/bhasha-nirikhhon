package com.bnau.cdb.repository;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;

import com.bnau.cdb.model.Company;
import com.bnau.cdb.model.Computer;

/**
 * Repository for computers.
 *
 * @author bertrand
 *
 */
@Repository
public interface ComputerRepository extends JpaRepository<Computer, Long> {

	@Transactional
	@Modifying
	void deleteByCompany(Company company);
}