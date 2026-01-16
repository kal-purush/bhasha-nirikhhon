package rw.review.controller;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.member.model.vo.Member;
import rw.review.model.service.BookService;

/**
 * Servlet implementation class BookLikeServlet
 */
@WebServlet("/bookLike.rw")
public class BookLikeServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public BookLikeServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String bookId = request.getParameter("bookId");
		
		HttpSession session = request.getSession();
		Member m = (Member)session.getAttribute("member");
		String memberNo = m.getMemberNo();
		
		//데이터가 있는지부터 판단
		boolean existResult = new BookService().existsBookLike(bookId,memberNo);
		
		RequestDispatcher view = request.getRequestDispatcher("/views/review/book_like_result.jsp");
		request.setAttribute("bookId", bookId);
		if(existResult) { //존재하면
			request.setAttribute("result", 0); //이미 있다고 보내기
		}else { //없으면
			int result = new BookService().insertBookLike(bookId,memberNo);
			if(result>0) { //내서재에 추가 완료
				request.setAttribute("result", 1);
			}else {
				request.setAttribute("result", -1);
			}
		}
		view.forward(request, response);
	
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}