package com.notable.controllers;

import java.util.List;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;

import com.notable.business.User;
import com.notable.data.UserMapper;

@Controller
public class LoginController {

	@Autowired
	JdbcTemplate jdbc;

	@PostMapping("login")
	public String loginUser(String email, String password, HttpServletResponse response, HttpServletRequest request) {

		// should only be one user in the list
		// Make email unique in the users table to ensure this
		List<User> users = jdbc.query("SELECT firstname, email, password FROM users WHERE email = '" + email + "'",
				new UserMapper());

		String emailResult = users.get(0).getEmail();
		;
		String passwordResult = users.get(0).getPassword();
		String firstNameResult = users.get(0).getFirstName();

		System.out.println(emailResult);
		System.out.println(passwordResult);
		System.out.println(firstNameResult);

		// verifying username and password, and if authenticated will create first name
		// cookie and login Cookie
		if (emailResult.equals(email) && passwordResult.equals(password)) {
			System.out.println("User is authenticated");

			Cookie firstNameCookie = new Cookie("firstNameCookie", firstNameResult);
			firstNameCookie.setMaxAge(60 * 60 * 24 * 365 * 2);
			firstNameCookie.setPath("/");
			response.addCookie(firstNameCookie);

			boolean makeLoginCookie = true;
			Cookie[] cookies = request.getCookies();
			for (Cookie cookie : cookies) {
				if (cookie.getName().equalsIgnoreCase("loggedInCookie")) {
					cookie.setValue("yes");
					cookie.setPath("/");
					response.addCookie(cookie);
					makeLoginCookie = false;
				}
			}
			if (makeLoginCookie) {
				Cookie loggedInCookie = new Cookie("loggedInCookie", "yes");
				loggedInCookie.setMaxAge(60 * 60 * 24 * 365 * 2);
				loggedInCookie.setPath("/");
				response.addCookie(loggedInCookie);
			}

//			Cookie[] cookies2 = request.getCookies();
//			for (Cookie cookieee : cookies2) {
//				System.out.println("c name: " + cookieee.getName());
//				System.out.println("c value: " + cookieee.getValue());
//			}	

		} else {
			// System.out.println("Wrong email and/or password!");
			return "views/loginInvalid";
		}

		return "index";
	}
}