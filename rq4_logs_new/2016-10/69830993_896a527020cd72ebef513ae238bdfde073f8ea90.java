package myDentist.web.controller;

import java.lang.reflect.Method;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import myDentist.model.Doctor;
import myDentist.model.User;
import myDentist.model.dao.doctorDao;
import myDentist.model.dao.userDao;

@Controller
public class doctorController {

	@Autowired
	private userDao userDao;
	
	@Autowired
	private doctorDao doctorDao;
	
	@RequestMapping(value="/doctorHome.html",method=RequestMethod.GET)
	public String doctorHome(ModelMap models,@RequestParam Integer userid)
	{
		models.put("userid", userid);
		System.out.println("user 1"+userid);
		return "doctorHome";
	}
	
	@RequestMapping(value="/doctorHome.html",method=RequestMethod.POST)
	public String doctorHome(@RequestParam Integer userid)
	{
		System.out.println("user 2"+userid);
		return "redirect:doctorHome.html?userid="+userid;
	}
	
	@RequestMapping(value="/doctorProfile.html",method=RequestMethod.GET)
	public String doctorProfile(ModelMap models,@RequestParam Integer userid)
	{
		System.out.println("user 3"+userid);
		models.put("userid", userid);
		models.put("users", userDao.getUsers());
		models.put("doctors", doctorDao.getDoctors());
		return "doctorProfile";
	}
	
	@RequestMapping(value="/doctorProfile.html",method=RequestMethod.POST)
	public String doctorProfile(@ModelAttribute ("doctors") Doctor doctor,BindingResult result,@RequestParam Integer userid)
	{
		User us=userDao.getUser(userid);
		doctor.setUserId(us);
		System.out.println(doctor.getDesignation());
		doctorDao.saveDoctor(doctor);
		return "redirect:doctorProfile.html?userid="+userid;
	}
	
}