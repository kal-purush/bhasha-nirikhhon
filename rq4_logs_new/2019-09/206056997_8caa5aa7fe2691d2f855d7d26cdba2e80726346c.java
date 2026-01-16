package br.edu.insper.controller;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.edu.insper.model.DAO;
import br.edu.insper.model.Message;
import br.edu.insper.model.Subject;
import br.edu.insper.model.User;

/**
 * Servlet implementation class chat
 */
@WebServlet("/chat")
public class chat extends HttpServlet {
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		RequestDispatcher dispatcher = request.getRequestDispatcher("chat.jsp");
		dispatcher.forward(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		DAO dao = new DAO();
		Message message = new Message();
		String fromMain = request.getParameter("fromMain");
		boolean main = Boolean.parseBoolean(fromMain);
		if (main) {
			String url = (String) request.getParameter("myURL");
			int idUser = (int) request.getSession().getAttribute("idUser");
			String newMsg = (String)request.getParameter("newMsg");
			String chatName = (String)request.getParameter("subjectName");

			if (!newMsg.isEmpty()) {
				Date date= new Date();
				long time = date.getTime();
				Timestamp ts = new Timestamp(time);
				message.setIdUser(idUser);
				message.setMsg(newMsg);
				message.setTime(ts);
				dao.addMessage(message, url);
				request.getSession().setAttribute("idUser", idUser);
				request.getSession().setAttribute("myURL", url);
				response.sendRedirect("chat?myURL="+url+"&idUser="+idUser+"&subjectName="+chatName+"&fromMain=false");
			} else {
				request.getSession().setAttribute("idUser", idUser);
				request.getSession().setAttribute("myURL", url);
				response.sendRedirect("chat");
			}

		} else {
			String url = (String) request.getParameter("myURL");
			String idUser1 = request.getParameter("idUser");
			int idUser = Integer.parseInt(idUser1);
			String newMsg = (String)request.getParameter("newMsg");
			String chatName = (String)request.getParameter("subjectName");

			if (!newMsg.isEmpty()) {
				Date date= new Date();
				long time = date.getTime();
				Timestamp ts = new Timestamp(time);
				message.setIdUser(idUser);
				message.setMsg(newMsg);
				message.setTime(ts);
				dao.addMessage(message, url);
				response.sendRedirect("chat?myURL="+url+"&idUser="+idUser+"&subjectName="+chatName+"&fromMain=false");
			} else {
				request.getSession().setAttribute("idUser", idUser);
				request.getSession().setAttribute("myURL", url);
				response.sendRedirect("chat");
			}
		}

	}
	// Parameters from user and actual chat
	//		String url = (String)request.getSession().getAttribute("myURL");


}