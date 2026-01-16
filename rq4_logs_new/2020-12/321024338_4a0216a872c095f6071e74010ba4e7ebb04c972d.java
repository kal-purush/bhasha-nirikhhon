package rw.library.controller;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rw.library.model.service.LibraryService;
import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;
import rw.review.model.vo.Book;

/**
 * Servlet implementation class LBCAddBookCaseServlet
 */
@WebServlet("/addBookCasePage.rw")
public class LBCAddBookCaseServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LBCAddBookCaseServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		LibraryService lService = new LibraryService();
		String libraryOwner = request.getParameter("libraryOwner");
		Member m = new MemberService().selectOneMemberId(libraryOwner);
		
		ArrayList<Book> listLB = lService.selectLikeBook(m.getMemberNo()); // 좋아요 한 책 리스트
		
		RequestDispatcher view = request.getRequestDispatcher("/views/library/book_case_add.jsp?libraryOwner="+libraryOwner);
		request.setAttribute("libraryOwner", libraryOwner);
		request.setAttribute("listLB", listLB);
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