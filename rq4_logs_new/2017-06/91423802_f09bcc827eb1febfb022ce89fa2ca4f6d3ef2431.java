package com.session;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.google.appengine.api.datastore.EntityNotFoundException;

public class UserInfoServlet extends HttpServlet {
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

		HttpSession session = req.getSession(false);
		String value = (String) session.getAttribute("email");
		try {
			SessionHelper.getUserInfo(value,resp);
		} catch (EntityNotFoundException e) {
			//resp.sendRedirect("dashboard");
		}

	}
}