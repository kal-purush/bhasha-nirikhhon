package uk.ac.bangor.cs.cambria.AcademiGymraeg.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.UserRepository;

/**
 * @author thh21bgf
 */

@Controller
@RequestMapping("/viewusers")
public class ViewUsersController {

	@Autowired
	private UserRepository repo;

	@GetMapping
	public String viewUsers(Model m) {
		m.addAttribute("allusers", repo.findAll());
		return "viewusers";
	}

}