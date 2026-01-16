package com.example.app.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.example.app.domain.User;

import jakarta.validation.Valid;

@Controller
@RequestMapping("/user")
public class UserController {
	@GetMapping("/register")
	public String register(Model model) {
		//ビューの方でドメインクラスが使えるように
		//ドメインクラス名　頭を小文字にした名前を指定すると
		//のちのち楽に記述ができる
		model.addAttribute("user" , new User());
		return "register";
	}
	@PostMapping("/register")
	// @ModelAttribute("user") User user
	public String register(
			@Valid User user,
			Errors errors
			) {
		//パスワードと確認用の比較
		if(!user.getPass().equals(user.getPassConf())) {
			//パスワードが違っていたらエラーコード作成
			//errors.rejectValue("フォームのname属性", "独自のエラーコード")
			
			errors.rejectValue("passConf", "passconf.wrong");
		}
		
		if(!errors.hasErrors()) {
			//エラーがなかった時の処理
			System.out.println("ユーザー登録");
		}
		return "register";
	}
	
	@GetMapping("/login")
	public String login(Model model) {
		model.addAttribute("user" , new User());
		return "login";
		
	}

}