package uk.ac.bangor.cs.cambria.AcademiGymraeg.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import jakarta.validation.Valid;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.User;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.UserRepository;

/**
 * @author ptg22svs
 */

/**
 * All requests to "/edituser" are handled by this controller
 */

@Controller
@RequestMapping("/edituser")
public class EditUserController {
	

	@Autowired
	private UserRepository repo;
		

	@GetMapping
	public String userEditPage(Model m) {

		if(!m.containsAttribute("user"))
		{
			User user = repo.findAll().get(0);
			
			m.addAttribute("user", user);
		}

		return "useredit";
	}


	@PostMapping
	public String editNoun(@Valid User u,  BindingResult result, Model m)  {

		if (result.hasErrors()) {
			m.addAttribute("user", u);
			return userEditPage(m);
		} else {

			var passwords = u.getPassword().split(",");
			
			if(!passwords[0].equals(passwords[1]))
				{
					result.rejectValue("password", "error.password", "The passwords do not match.");
					m.addAttribute("user", u);
					return userEditPage(m);
				}
			

			repo.save(u);			
			
			return "redirect:/home";
		}
				

	}
	

}