package myDentist.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import myDentist.model.Appointments;
import myDentist.model.dao.appointmentsDao;
import myDentist.model.dao.patientDao;
import myDentist.model.dao.userDao;

@Controller
public class appointmentController {
	
	@Autowired
	private userDao userDao;
	
	@Autowired
	private appointmentsDao appointmentsDao;
	
	@Autowired
	private patientDao patientDao;
	
	@RequestMapping(value="/appointment.html", method=RequestMethod.GET)
	public String takeAppointment(ModelMap models)
	{
		models.put("users", userDao.getUsers());
		models.put("appointments", new Appointments());
		return "appointment";
		
	}
	
	@RequestMapping(value="/appointment.html",method=RequestMethod.POST)
	public String takeAppointment(@ModelAttribute("appointments") Appointments appointment,@RequestParam Integer id,BindingResult results)
	{		
		System.out.println("-------------"+id);
		//appointment.setDoctorId("10");
		appointment.setPatientId(userDao.getUser(id));
		
		System.out.println("value is :"+appointment.getAppointmentDate());
		appointment=appointmentsDao.saveAppointment(appointment);
		return "redirect:display.html";		
	}

}