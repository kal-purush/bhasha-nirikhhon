package com.project.Link.Community.Controller;

import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.ui.Model;

public interface CommunityController {

	public HashMap<String, String> sessionControl(HttpSession session);
	public String ListCommunities(Model model, HttpServletRequest request, HttpSession session);
	public String PostCommunity(Model model, HttpServletRequest request, HttpSession session);
	public String GetCommunity(Model model, HttpServletRequest request, HttpSession session);
	public String UpdateCommunity(Model model, HttpServletRequest request, HttpSession session);
	public String DeleteCommunity(Model model, HttpServletRequest request, HttpSession session);
}