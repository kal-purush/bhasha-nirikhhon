package com.Polodz.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import com.Polodz.model.StringWebData;

//@Controller
public class WebMovieListController {

	@GetMapping("/stringForm")
	public String stringForm(Model model) {
		model.addAttribute("stringgg", new StringWebData());
		return "stringForm";
	}

	@PostMapping("/stringForm")
	public String stringSubmit(StringWebData stringgg) {
		return "stringSubmit";
	}
}