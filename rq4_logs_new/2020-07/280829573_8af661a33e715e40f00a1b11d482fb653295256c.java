package com.LinkeT.LinkeT.Team.Controller;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.ui.Model;

import com.LinkeT.LinkeT.Team.Team;
import com.LinkeT.LinkeT.Team.Dao.TeamDao.usrin;

// teamlist는 별도로 관리할것

public interface TeamController {
	public int teamCreate(Model model, HttpServletRequest request, HttpSession session);
	public String teamGet(Model model, HttpServletRequest request, HttpSession session);
}