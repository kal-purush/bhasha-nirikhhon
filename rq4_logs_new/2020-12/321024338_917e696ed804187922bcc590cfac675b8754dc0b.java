package rw.library.controller;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.library.model.service.LibraryService;
import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class LBCDeleteBookCaseServlet
 */
@WebServlet("/deletBookCase.rw")
public class LBCDeleteBookCaseServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LBCDeleteBookCaseServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		Member member = (Member)session.getAttribute("member");
		
		String libraryOwner = request.getParameter("libraryOwner");
		String bookShelfId = request.getParameter("bookShelfId");
		Member m = new MemberService().selectOneMemberId(libraryOwner); // 책장주인 정보
		
		LibraryService lService = new LibraryService();
		
		if(m.getMemberId().equals(member.getMemberId())) { 
			int result = lService.deleteBookCase(m.getMemberNo(), bookShelfId);
			if(result>0) { // 삭제 성공
				RequestDispatcher view = request.getRequestDispatcher("/myBookCase.rw?libraryOwner="+libraryOwner);
				view.forward(request, response);
			}else { // 삭제 실패
				
			}
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