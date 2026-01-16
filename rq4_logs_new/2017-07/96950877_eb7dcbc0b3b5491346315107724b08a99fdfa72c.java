package com.cicdproject.BankPortal.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Repository;

import com.cicdproject.BankPortal.model.BankAccountInterface;

@Repository
public class BankAccountDAOInterfaceImpl implements BankAccountDAOInterface {
	
	@PersistenceContext
	private EntityManager entityManager;

	@Override
	public void create(BankAccountInterface bankAccount) {
		entityManager.persist(bankAccount);
	}

	@Override
	public void update(BankAccountInterface bankAccount) {
		entityManager.merge(bankAccount);
	}

	@Override
	public BankAccountInterface getUserAccountByUserName(String email) {
		
		return entityManager.find(BankAccountInterface.class, email);
	}

	@Override
	public void delete(String email) {
		BankAccountInterface bankReference=getUserAccountByUserName(email);
		if (bankReference != null) {
            entityManager.remove(bankReference);
        } else {
        	// log that the requested ID did not exist
        	System.out.printf("Delete Error: User Acount %s does not exist.\n", email);
        }
		
	}
	
	public List<BankAccountInterface> getAllUserAccounts() {
		// This query is the equivelent of "SELECT * FROM USER_ACCOUNTS"
		// no idea why we use "from <entity type>", but it works
		return entityManager.createQuery("from BankAccountInterface", BankAccountInterface.class).getResultList();
	}
	

}