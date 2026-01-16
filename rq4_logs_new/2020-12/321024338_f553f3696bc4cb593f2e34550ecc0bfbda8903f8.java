package rw.inquiry.controller;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.inquiry.model.service.InquiryService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class InquirySendServlet
 */
@WebServlet("/sendInquiry.rw")
public class InquirySendServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public InquirySendServlet() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// 1.인코딩
		request.setCharacterEncoding("UTF-8");

		// 2. 자바코드로 저장
		String category = request.getParameter("category");
		String email = request.getParameter("emailSendEmail");
		String title = request.getParameter("inquiryTitle");
		String content = request.getParameter("inquiryContent");
		System.out.println(category);
		
		HttpSession session = request.getSession();
		Member m = (Member)session.getAttribute("member");
		String memberId = m.getMemberId();

		// 3. 비지니스 로직 처리
		int result = new InquiryService().insertInquiry(memberId, category, email, title, content);
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html; charset=UTF-8");
		PrintWriter out = response.getWriter();

		if (result > 0) {
			out.println("<script>alert('작성이 완료되었습니다. 답변은 추후 이메일로 발송 예정입니다.');</script>");
			out.println("<script>location.replace('/customer_center.rw')</script>");
		} else {
			out.println("<script>alert('문의 작성을 실패하였습니다.');</script>");
			out.println("<script>history.back(-1);</script>");
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}