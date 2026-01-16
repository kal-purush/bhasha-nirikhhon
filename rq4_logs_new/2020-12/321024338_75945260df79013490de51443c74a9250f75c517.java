package rw.library.controller;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.simple.JSONObject;

import rw.library.model.service.LibraryService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class LBCDeleteBookServlet
 */
@WebServlet("/delBookInCase.rw")
public class LBCDeleteBookServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LBCDeleteBookServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		if((Member)session.getAttribute("member")!=null) {
			String bookShelfId = request.getParameter("bookShelfId");
			String bookId = request.getParameter("bookId");
			
			int result = new LibraryService().deleteBookInCase(bookShelfId,bookId);
			
			JSONObject object = new JSONObject();
			
			if(result>0) {
				object.put("result", true);
			}else {
				object.put("result", false);
			}
			response.setContentType("application/json");
			
			PrintWriter out = response.getWriter();
			
			out.print(object);
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}