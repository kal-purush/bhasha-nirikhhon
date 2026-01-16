package rw.review.controller;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rw.review.model.service.BookService;
import rw.review.model.vo.BookReview;

/**
 * Servlet implementation class BookLoadServlet
 */
@WebServlet("/bookInfo.rw")
public class BookLoadServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public BookLoadServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String bookId = request.getParameter("bookId"); //bookId 받아오기
		
		//작업 1) bookId 그대로 넘겨주기
		//작업 2) book과 연결된 리뷰 목록 가져오기
		ArrayList<BookReview> list = new BookService().selectReviewList(bookId);
		//작업 3) book과 연결된 리뷰 평점 계산
		double average = new BookService().getReviewRateAvg(bookId);
		
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html; charset=UTF-8"); 
		
		RequestDispatcher view = request.getRequestDispatcher("/views/review/book_detail.jsp?bookId="+bookId);
		request.setAttribute("bookId", bookId);
		request.setAttribute("reviewList", list);
		request.setAttribute("avg", average);
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