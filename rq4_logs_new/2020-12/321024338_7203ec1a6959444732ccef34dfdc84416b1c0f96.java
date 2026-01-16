package rw.col.controller;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.simple.JSONObject;

import rw.col.model.service.CollectionService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class BookshelfCollectionDeleteServlet3
 */
@WebServlet("/boolshelfCollectionDel3.rw") // 남의 서재 - 책장에서 책장 컬렉션 제거
public class BookshelfCollectionDeleteServlet3 extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public BookshelfCollectionDeleteServlet3() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		Member member = (Member)session.getAttribute("member");
		
		String bookCaseId = request.getParameter("bookCaseId"); // 책장ID
		String memberNo = member.getMemberNo(); // 내 회원ID
		
		int result = new CollectionService().deleteBookCase(memberNo,bookCaseId);
		
		JSONObject object = new JSONObject();
		//JSONObject는 map형태에서 파생되었음 (key,value)
		
		object.put("result", result);
		// 위처럼 작성하게 되면 JSON 형태로 처리되었다고 생각하면 됨
		// {"name":m.getName(),"age":m.getAge(),"addr":m.getAddr()}
		
		// JSON 객체를 보내주기 전에 해당 ajax에 데이터 타입을 알려주어야 함
		response.setContentType("application/json");
		
		PrintWriter out = response.getWriter();
		
		out.print(object);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}