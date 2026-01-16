package myDentist.model.dao.jpa;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import myDentist.model.Appointments;
import myDentist.model.dao.appointmentsDao;
import myDentist.model.dao.userDao;


@Repository
public class appointmentsDaoImpl implements appointmentsDao{

	@PersistenceContext
	private EntityManager entitymanager;

	
	@Override
	@Transactional
	public Appointments saveAppointment(Appointments appointment) {
		
		return entitymanager.merge(appointment);
	}

	@Override
	public Appointments getAppointment(Integer patientId) {
		
		return entitymanager.find(Appointments.class,patientId);
	}
}