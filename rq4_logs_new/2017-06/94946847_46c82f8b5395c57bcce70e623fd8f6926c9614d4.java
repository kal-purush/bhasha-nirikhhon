package com.sulphur.user.controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import com.sulphur.connectmysql.ConnectMysql;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/user")
public class Editonline {
	Connection conn;
	
	@RequestMapping(value="/upload",method=RequestMethod.GET)
	
	public String upload( HttpServletRequest req,@Requestparam("团队") String t_name,@Requestparam() String p_name,
			@Requestparam() String t_leader,@Requestparam() String telephone,@Requestparam() String mail,
			@Requestparam() String p_progress,@Requestparam() String harvest,@Requestparam() String nextaim)
	throws SQLException
	{
		conn = new ConnectMysql().getConnection();
	    PreparedStatement cnm=conn.prepareStatement("insert into upload values(?,?,?,?,?,?,?,?)");
		cnm.setString(1, t_name);
		cnm.setString(2, p_name);
		cnm.setString(3, t_leader);
		cnm.setString(4, telephone);
		cnm.setString(5, mail);
		cnm.setString(6, p_progress);
		cnm.setString(7, harvest);
		cnm.setString(8, nextaim);
		cnm.executeQuery();
		cnm.close();
		
		
		
		
		
		
		
		
		
		return new ModelAndView("/user/on-line editing.jsp","message",1);
	
	
	
}
	
	
	
	
	
	
}