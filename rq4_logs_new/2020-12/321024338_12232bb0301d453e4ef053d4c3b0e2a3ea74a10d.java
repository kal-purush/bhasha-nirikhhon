package rw.member.controller;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class MemberUpdateServlet
 */
@WebServlet("/modifyInfo.rw")
public class MemberUpdateServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public MemberUpdateServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setCharacterEncoding("UTF-8");
		
		String memberPwd = request.getParameter("memberPwd");
		
		HttpSession session = request.getSession();
		Member m = (Member)session.getAttribute("member");
		
		if(memberPwd.equals(m.getMemberPwd())) {
			String memberId = m.getMemberId();
			String nickName = request.getParameter("nickName");
			int birthYear = Integer.parseInt(request.getParameter("birthYear"));
			char gender = request.getParameter("gender").charAt(0);
			
			m.setMemberId(memberId);
			m.setNickname(nickName);
			m.setBirthYear(birthYear);
			m.setGender(gender);
			
			int result = new MemberService().updateMember(m);
			
			response.setCharacterEncoding("UTF-8");
			response.setContentType("text/html; charset=UTF-8;");
			PrintWriter out = response.getWriter();
			
			if(result > 0) {
				out.print("<script>alert('수정이 완료되었습니다.');</script>");
				out.print("<script>location.replace('/pageLoad.rw');<script>");
			} else {
				out.print("<script>alert('수정이 정상적으로 처리되지 못했습니다.(지속적인 문제 발생 시 관리자에게 문의해주세요.)');</script>");
			}
 			
		} else {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("text/html; charset=UTF-8;");
			PrintWriter out = response.getWriter();
			
			out.print("<script>alert('비밀번호가 일치하지 않습니다.');</script>");
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