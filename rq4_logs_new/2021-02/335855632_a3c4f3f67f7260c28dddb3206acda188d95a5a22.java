
package com.dynamicdudes.dao;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import com.dynamicdudes.model.LoanApplicant;


//@Component
@Repository
public class LoanApplicantDAOImpl implements LoanApplicantDAO {
	
	@Autowired
	SessionFactory sessionFactory;

	@Override
	public void saveLoanApplicant(LoanApplicant loanApplicant) {
		
		Session currentSession = sessionFactory.getCurrentSession();
		
		currentSession.save(loanApplicant);
		
	}

	@Override
	public List<LoanApplicant> getLoanApplicants() {
		
		Session currentSession = sessionFactory.getCurrentSession();
		
		Query<LoanApplicant> query = currentSession
				.createQuery("from LoanApplicant",LoanApplicant.class);
		
		return query.getResultList();
	}

	@Override
	public LoanApplicant getLoanApplicantById(int applicationId) {
		
       Session currentSession = sessionFactory.getCurrentSession();
		
		return currentSession.get(LoanApplicant.class, applicationId);
	}

}