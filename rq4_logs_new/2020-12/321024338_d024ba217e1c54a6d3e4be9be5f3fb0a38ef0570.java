package rw.library.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.library.model.service.LibraryService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class LBCUpdateLockServlet
 */
@WebServlet("/bookCaseModifyLock.rw")
public class LBCUpdateLockServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LBCUpdateLockServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		char lockData = request.getParameter("lockData").charAt(0);
		String bookShelfId = request.getParameter("bookShelfId");
		
		HttpSession session = request.getSession();
		String memberNo = ((Member)session.getAttribute("member")).getMemberNo();
		
		if(lockData=='N') { // 현 상태가 N라면 == 현재 상태 열림
			lockData = 'Y';
			new LibraryService().updateLock(memberNo, bookShelfId, lockData);
		}else { // 현 상태가 Y이라면
			lockData = 'N';
			new LibraryService().updateLock(memberNo, bookShelfId, lockData);
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