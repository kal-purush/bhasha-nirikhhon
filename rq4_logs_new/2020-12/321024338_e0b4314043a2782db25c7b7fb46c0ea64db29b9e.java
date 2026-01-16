package rw.admin.inquiry.controller;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rw.admin.inquiry.model.service.InquiryService;
import rw.common.MailAuthentication;

/**
 * Servlet implementation class InsertInquiryServlet
 */
@WebServlet("/insertInquiry.ad")
public class InquiryInsertServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public InquiryInsertServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setCharacterEncoding("UTF-8");

		String email = request.getParameter("ReplyEmail");
		String title = request.getParameter("answerTitle");
		String content = request.getParameter("answerContent");
		
		int result = new MailAuthentication().sendMailInquriy(email, title, content);
		
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html; charset=UTF-8");
		PrintWriter out = response.getWriter();
		
		if(result == 1) { //성공
			int update = new InquiryService().update(email);
			out.println("<script>alert('메일 전송이 완료되었습니다.');</script>");
			out.println("<script>location.replace('/selectAllInquiry.ad');</script>");
		}else {
			out.println("<script>alert(메일 전송이 실패되었습니다.);</script>");
			out.println("<script>history.back(-1);</script>");
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