package com.example.app.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import com.example.app.service.MachineSetCountService;

import lombok.RequiredArgsConstructor;

@Controller
@RequiredArgsConstructor
public class MachineSetCountController {

	private final MachineSetCountService service;

	@GetMapping("/show")
	public String allList(Model model) throws Exception{

		return "show_aramaki";
	}
}