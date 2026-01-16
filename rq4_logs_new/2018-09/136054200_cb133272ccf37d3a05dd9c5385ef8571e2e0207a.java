package com.lab206.controllers;

import java.security.Principal;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.lab206.models.Badge;
import com.lab206.models.BadgeRequest;
import com.lab206.models.User;
import com.lab206.services.AnnouncementService;
import com.lab206.services.BadgeRequestService;
import com.lab206.services.BadgeService;
import com.lab206.services.QuicklinkService;
import com.lab206.services.UserService;

@Controller
public class BadgeController {
	
	private BadgeService bs;
	private UserService us;
	private BadgeRequestService brs;
	private AnnouncementService as;
	private QuicklinkService qs;
	
	public BadgeController(BadgeService bs,
			UserService us,
			BadgeRequestService brs,
			AnnouncementService as,
			QuicklinkService qs) {
		this.bs = bs;
		this.us = us;
		this.brs = brs;
		this.as = as;
		this.qs = qs;
	}
	
	@RequestMapping("/badge/{id}")
	public String showBadge(@PathVariable("id") Long id,
			Principal principal,
			Model model) {
		Badge badge = bs.findById(id);
		User currentUser = us.findByEmail(principal.getName());
		model.addAttribute("badge", badge);
		model.addAttribute("currentUser", currentUser);
		model.addAttribute("announcements", as.findAll());
		model.addAttribute("quicklinks", qs.findAll());
		model.addAttribute("request", brs.findByUserAndBadge(currentUser, badge));
		return "badge.jsp";
	}
	
	@PostMapping("/badge/request")
	public String requestBadge(@ModelAttribute("badgeRequest") BadgeRequest badgeRequest,
			@RequestParam("badgeId") Long badgeId,
			Principal principal) {
		User currentUser = us.findByEmail(principal.getName());
		Badge badge = bs.findById(badgeId);
		brs.saveBadgeRequest(badgeRequest, currentUser, badge);
		return "redirect:/badge/" + badgeId;
	}

}