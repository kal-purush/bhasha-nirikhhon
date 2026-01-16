package com.sto.controllers;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.sto.entities.Role;
import com.sto.entities.Users;
import com.sto.repositories.RoleRepository;
import com.sto.repositories.UserRepository;

@Controller
public class UserController {

	@Autowired
	private UserRepository userRepository;

	
	@Autowired
	private RoleRepository roleRepository;

	

	@RequestMapping(value = "/user", params = "add", method = RequestMethod.GET)
	public String getAddUser() {
		return "user/add";
	}

	@RequestMapping(value = "/user", params = "edit", method = RequestMethod.GET)
	public String getEditUser(@RequestParam long id, Model model) {
		model.addAttribute("user", userRepository.findOne(id));
		return "user/edit";
	}
	@RequestMapping(value = "/user", method = RequestMethod.GET)
	public String getViewUser(@RequestParam long id, Model model) {
		model.addAttribute("user", userRepository.findOne(id));
		return "user/view";
	}

	
	@RequestMapping(value = "/user", params = "add", method = RequestMethod.POST)
	public String postAddUser(@RequestParam String userID, @RequestParam String firstName, 
			@RequestParam String lastName,
			@RequestParam String password, @RequestParam String email,
			@RequestParam String dob, @RequestParam String roles, @RequestParam String status) {	
		Users user = new Users(userID, firstName, lastName, email, password, dob, status);
		List<Users> listUser = new ArrayList<Users>();
		listUser.add(user);
		List<Role> listRole = new ArrayList<Role>();
		Role role = new Role(roles, listUser);
		listRole.add(role);
		role  = roleRepository.save(role);
		user.setRoles(listRole);
		user = userRepository.save(user);
		return "redirect:user?id=" + user.getId();
	}

	@RequestMapping(value = "/user", params = "edit", method = RequestMethod.POST)
	public String postEditUser(@RequestParam long id,
			@RequestParam String userID, @RequestParam String firstName, @RequestParam String lastName,
			@RequestParam String password,
			@RequestParam String email, @RequestParam String dob,
			@RequestParam String roles) {
		Users user = userRepository.findOne(id);
		user.setUserID(userID);
		user.setFirstName(firstName);
		user.setLastName(lastName);
		user.setPassword(password);
		user.setEmail(email);
		user.setDob(dob);
		user.setRoles(null);
		userRepository.save(user);

		return "redirect:user?id=" + user.getId();
	}

	@RequestMapping(value = "/user", params = "delete", method = RequestMethod.POST)
	public String postDeleteUser(@RequestParam Long id) {
		userRepository.delete(id);

		return "redirect:users";
	}

}