package com.example.demo.Controller;

import java.util.List;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import com.example.demo.Model.BackgroundSync;
import com.example.demo.Model.TurbohireDummy;
import com.example.demo.Service.InterviewerService;

@org.springframework.web.bind.annotation.RestController

@Controller
public class InterviewerController {

	@Autowired
	private InterviewerService service;
	

	@PostMapping(value="/save-backgroundSync")
	@Transactional
	public void backgoundSyncData( @RequestBody BackgroundSync backgroundsync) {
		service.SaveBackgroundSync(backgroundsync) ;

		}
	@GetMapping("/all-user")
	public Iterable<BackgroundSync> showAllUsers(){
		return service.showAllInterviewers();
	}
	
	
	@PostMapping(value="/save-turbohire")
	@Transactional
	public void turboHireData( @RequestBody TurbohireDummy turbohiredummy) {
		service.SaveTurbohireDummy(turbohiredummy) ;

		}
}