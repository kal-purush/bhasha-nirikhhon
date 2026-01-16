package com.se.fit.TravelProject.controller;

import java.security.SecureRandom;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.se.fit.TravelProject.entities.Account;
import com.se.fit.TravelProject.entities.User;
import com.se.fit.TravelProject.service.AccountService;
import com.se.fit.TravelProject.service.SendMailService;
import com.se.fit.TravelProject.service.UserService;

import jakarta.validation.Valid;

@Controller
@RequestMapping("/Account")
public class AccountController {
	private AccountService accountService;
	private UserService userService;
	private SendMailService mailService;

	@Autowired
	public AccountController(AccountService accountService, UserService userService, SendMailService mailService) {
		super();
		this.accountService = accountService;
		this.userService = userService;
		this.mailService = mailService;
	}

	@GetMapping("/showFormAccount")
	public String showFormAccount(Model model, @RequestParam("USERNAME") String username) {
		Account account = new Account();
		model.addAttribute("username", username);
		model.addAttribute("account", account);
		return "ChangePass";
	}

	@PostMapping("/savePass")
	public String savePass(@Valid @ModelAttribute("account") Account account, BindingResult result,
			@RequestParam("userId") int id) {
		if (result.hasErrors()) {
			return "ChangePass";
		}
		User user = userService.getUserById(id);
		account.setUser(user);
		accountService.saveUser(user, account);
		return "redirect:/";
	}

	@GetMapping("/showFormForgot")
	public String showFormForgot(Model model) {
		Account account = new Account();
		model.addAttribute("account", account);
		return "ForgotPass";
	}

	private static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final String LOWER = "abcdefghijklmnopqrstuvwxyz";
	private static final String DIGITS = "0123456789";
	private static final String ALL = UPPER + LOWER + DIGITS;

	public static String generateRandomPassword() {
		SecureRandom random = new SecureRandom();

		int length = random.nextInt(9) + 8; // Độ dài từ 8 đến 16 ký tự

		StringBuilder password = new StringBuilder(length);

		// Bắt đầu mật khẩu với chữ hoa
		password.append(UPPER.charAt(random.nextInt(UPPER.length())));

		// Sinh ngẫu nhiên phần còn lại của mật khẩu
		for (int i = 1; i < length; i++) {
			password.append(ALL.charAt(random.nextInt(ALL.length())));
		}

		return password.toString();
	}

	@GetMapping("/sendPass")
	public String sendPass(@RequestParam("emailUser") String emailUser, @RequestParam("userName") String userName) {
		Account account = accountService.getAccountById(userName);
		User user = account.getUser();
		
		String pass = generateRandomPassword();
		account.setPassword(pass);
		account.setUser(user);
		accountService.saveUser(user, account);
		
		String name = user.getFullName();
		mailService.sendPassNew(emailUser, name, pass);
		
		return "dangnhap";
	}

}