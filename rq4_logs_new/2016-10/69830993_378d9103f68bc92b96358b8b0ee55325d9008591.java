package myDentist.model.dao.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Repository;

import myDentist.model.Appointments;
import myDentist.model.Doctor;
import myDentist.model.Patient;
import myDentist.model.dao.doctorDao;

@Repository
public class doctorDaoImpl implements doctorDao{

	@PersistenceContext
	private EntityManager entitymanager;
	
	@Override
	public Doctor getDoctor(Integer doctorId) {
	
		return entitymanager.find(Doctor.class,doctorId);
	}

	@Override
	public List<Doctor> getDoctors() {
		
		return entitymanager.createQuery("from Doctor order by doctorId", Doctor.class).getResultList();
	}

	
}