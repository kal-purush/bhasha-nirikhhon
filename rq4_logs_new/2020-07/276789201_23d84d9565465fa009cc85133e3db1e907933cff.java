package com.javaex.controller;

import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.javaex.dao.GuestDao;
import com.javaex.vo.GuestVo;

@Controller
@RequestMapping("/gb")
public class GuestController {
	GuestDao guestDao = new GuestDao();
	
	@RequestMapping(value = "/list", method = {RequestMethod.GET, RequestMethod.POST})
	public String list(Model model) {
		System.out.println("/gb/list");
		
		List<GuestVo> gList = guestDao.getGuestList();
		System.out.println(gList.toString());
		
		model.addAttribute("gList", gList);
		
		return "/WEB-INF/views/addList.jsp";
		
	}
	
	@RequestMapping(value = "/add", method = {RequestMethod.GET, RequestMethod.POST})
	public String add(@ModelAttribute GuestVo guestVo) {
		System.out.println("/gb/add");
		System.out.println(guestVo.toString());
		
		guestDao.contentsInsert(guestVo);
		
		return "redirect:/gb/list";
	}
	
	@RequestMapping(value = "/delete", method = {RequestMethod.GET, RequestMethod.POST})
	public String delete(@ModelAttribute GuestVo guestVo) {
		System.out.println("/gb/delete");
		System.out.println(guestVo.toString());
		
		guestDao.guestDelete(guestVo);
		
		return "redirect:/gb/list";
	}
	
	@RequestMapping(value = "/deleteForm", method = {RequestMethod.GET, RequestMethod.POST})
	public String deleteForm(Model model, @RequestParam("no") int no) {
		System.out.println("/gb/deleteForm");
		model.addAttribute("no", no);
		
		return "/WEB-INF/views/deleteForm.jsp";
	}

}