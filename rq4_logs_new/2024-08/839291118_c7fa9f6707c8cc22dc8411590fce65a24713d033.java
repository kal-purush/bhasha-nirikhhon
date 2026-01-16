package com;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import models.*;

/**
 * Класс Comments наследует от абстрактного класса HttpServlet, переопределяя его методы doGet, doPost и doDelete.<br>
 * Сервлет Comments отвечает за просмотр, добавление и удаление комментариев к статье.
 */
@WebServlet(urlPatterns= {"/comments", "/comment"})
public class Comments extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	/**
	* Метод doGet сервлета Comments. Отбражает страницу с комментариями к статье.<br>
	* Если Get-запрос пришел по адресу /comments?delete={comIdx}&id={articleId}, вызывает метод doDelete.<br>
	* В обратном случае, передает запрос на страницу /article.jsp.<br>
	* <b>Получает атрибуты запроса: "articleId".</b>
	* <b>Устанавливает атрибуты запроса: "comments", "commentators".</b>
	* @param request - HTTP запрос
	* @param response - HTTP ответ
	*/
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("Метод doGet сервлета Comments"); //DEBUG 
		
		if ( request.getParameter("delete") != null ) {
			doDelete(request, response);
			
		} else {
			Article article = (Article) request.getAttribute("article");
			
			dbFunctions DB = new dbFunctions();
			Connection conn = DB.connectToDB();
			
			ArrayList<Comment> comments = DB.getComments(conn, article.getId());
			request.setAttribute("comments", comments);
			
			HashMap<Integer, User> commentators = new HashMap<>();
			for (Comment comment: comments) {
				User user = DB.getUser(conn, comment.getAuthor());
				commentators.put(comment.getId(), user);
			}
			request.setAttribute("commentators", commentators);
			
			request.getServletContext().getRequestDispatcher("/article.jsp").forward(request, response);
		}
		
	}
	
	/**
	* Метод doPost сервлета Comments. Получает параметры формы комментирования и создает новый комментарий.<br>
	* Перенаправляет пользователя на страницу комментируемой статьи.<br>
	* <b>Получает параметры запроса: "articleId", "author", "authorAvatar", "comment".</b><br>
	* @param request - HTTP запрос
	* @param response - HTTP ответ
	*/
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("Метод doPost сервлета Comments"); //DEBUG
		
		Integer articleId = Integer.valueOf(request.getParameter("articleId"));
		String author = request.getParameter("author");
		String comment = request.getParameter("comment");
		
		dbFunctions DB = new dbFunctions();
		Connection conn = DB.connectToDB();
		
		DB.createComment(conn, author, articleId, comment);
		
		response.sendRedirect(request.getContextPath() + "/feed/" + articleId);
	}
	
	/**
	* Метод doDelete сервлета Comments. Если пользователь является автором комментария, удаляет комментарий.<br>
	* Перенаправляет пользователя на страницу просматриваемой статьи.<br>
	* <b>Получает параметр запроса "delete".</b><br>
	* <b>Получает атрибут сессии "user".</b>
	* @param request - HTTP запрос
	* @param response - HTTP ответ
	*/
	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("Метод doDelete сервлета Comments"); //DEBUG
		
		User user = (User) request.getSession().getAttribute("user");
		
		Integer commId = Integer.valueOf(request.getParameter("delete"));
		
		dbFunctions DB = new dbFunctions();
		Connection conn = DB.connectToDB();
		
		Comment comment = DB.getComment(conn, commId);
		String commentAuthor = comment.getAuthor();
		
		if ( user.getUsername().equalsIgnoreCase(commentAuthor) ) {
			DB.deleteComment(conn, commId);
		}
		
		response.sendRedirect(request.getContextPath() + "/feed/" + comment.getArticleId());
	}

}