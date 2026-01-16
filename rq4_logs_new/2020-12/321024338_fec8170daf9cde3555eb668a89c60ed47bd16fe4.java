package rw.library.controller;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.library.model.service.LibraryReviewNoteService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class MyLibraryReviewNoteServlet
 */
@WebServlet("/myLibrary.rw")
public class LibraryReviewNoteServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LibraryReviewNoteServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		String memberNo = ((Member)session.getAttribute("member")).getMemberNo();
		// System.out.println("[내 서재] 내 멤버고유번호 : "+memberNo);
		Member m = new LibraryReviewNoteService().selecAlltMyLibraryHeader(memberNo); // 내 서재 헤더 부분
		
		//MLRVNote == MyLibraryReviewNote
		
		
		if(m!=null) {
			session.setAttribute("member", m);
		}
		response.sendRedirect("/views/library/review_note/reviewNote.jsp");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
}