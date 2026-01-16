package admin.controller;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import bike.model.vo.Bike;
import member.model.vo.Member;

/**
 * Servlet implementation class BikeShopSearchServlet
 */
@WebServlet("/bikeShopSearch")
public class BikeShopSearchServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public BikeShopSearchServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Member m = (Member)request.getSession().getAttribute("memberLoggedIn");
		// 로그인했지만 admin아닌경우 ㅡㅡ 로그인 안됨
			String searchType = request.getParameter("searchType");
			System.out.println(searchType);
			String searchKeyword = request.getParameter("searchKeyword");
			System.out.println(searchKeyword);
			List<Bike> list = null;
			
			switch (searchType) {
			
			

			}
			int cPage = Integer.parseInt(request.getParameter("cPage"));
			int numPerPage = Integer.parseInt(request.getParameter("numPerPage"));
			
			/*String pageBar = new PageBar().getPageBar(request,cPage,numPerPage);*/
			request.setAttribute("searchType", searchType);
			request.setAttribute("searchKeyword", searchKeyword);
			request.setAttribute("cPage", cPage );
			request.setAttribute("numPerPage", numPerPage);
			/*request.setAttribute("pageBar", pageBar);*/
			request.setAttribute("list", list);
			request.getRequestDispatcher("/views/admin/BikeList.jsp").forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}