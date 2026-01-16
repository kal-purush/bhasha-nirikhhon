package com.team2.lb.controller;

import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.team2.lb.service.BookBoardService;
import com.team2.lb.vo.BookBoard;

import lombok.extern.slf4j.Slf4j;

@Controller
@Slf4j
@RequestMapping("bookBoard")
public class BookBoardController {
	
	@Autowired
	BookBoardService service;
	
	@GetMapping("sellBoard")
	public String sellBoard() {
		return "bookBoard/sellBoard";
	}
	
	@PostMapping("registSell")
	public String registSell(BookBoard bookBoard, @AuthenticationPrincipal UserDetails user) {
		log.debug("결과는:{}" ,bookBoard);
		bookBoard.setId(user.getUsername());
		int result = service.registSell(bookBoard);
		return "redirect:/";
	}
	
	@GetMapping("boardList")
	public String boardList(Model model) {
		ArrayList<BookBoard> boardList = service.showBoardList();
		model.addAttribute("boardList", boardList);
		return "bookBoard/boardList";
	}

}