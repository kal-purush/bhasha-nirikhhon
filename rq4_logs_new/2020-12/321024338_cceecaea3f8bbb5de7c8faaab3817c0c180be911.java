package rw.member.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class MemberMyPageServlet
 */
@WebServlet("/pageLoad.rw")
public class PageLoadServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public PageLoadServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		
		Member m = (Member)session.getAttribute("member");
		String memberId = m.getMemberId();
		String memberPwd = m.getMemberPwd();
		
		// 갱신하기 위한 비즈니스 로직
		m = new MemberService().loginMember(memberId, memberPwd);
		session.setAttribute("member", m); // session 갱신
		
		response.sendRedirect("/views/member/modify_info.jsp");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}