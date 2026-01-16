package rw.member.controller;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class memberInsertServlet
 */
@WebServlet("/memberJoin.rw")
public class memberInsertServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public memberInsertServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setCharacterEncoding("utf-8");
		
		Member m = new Member();
		
		try {
			String memberId = request.getParameter("memberId");
			String nickName = request.getParameter("nickName");
			String memberPwd = request.getParameter("memberPwd");
			String email = request.getParameter("email");
			int birthYear = Integer.parseInt(request.getParameter("birthYear"));
			char gender = request.getParameter("gender").charAt(0);
			
			m.setMemberId(memberId);
			m.setNickname(nickName);
			m.setMemberPwd(memberPwd);
			m.setEmail(email);
			m.setBirthYear(birthYear);
			m.setGender(gender);
			
			int result = new MemberService().insertMember(m);
			
			RequestDispatcher view = request.getRequestDispatcher("/views/member/memberJoinResult.jsp");
			if(result > 0) {
				request.setAttribute("result", true);
			} else {
				request.setAttribute("result", false);
			}
			view.forward(request, response);
			
		} catch(Exception e) {
			RequestDispatcher view = request.getRequestDispatcher("/views/member/memberJoinResult.jsp");
			request.setAttribute("result", false);
			view.forward(request, response);
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