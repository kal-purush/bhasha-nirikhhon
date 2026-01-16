package lent.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lent.model.service.LentService;
import lent.model.vo.LentCancel;

/**
 * Servlet implementation class LentCancelAdminServlet
 */
@WebServlet("/lent/lentCancelAdmin")
public class LentCancelAdminServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LentCancelAdminServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// LentCancelAdminServlet
		String cancelMuid = (String) request.getParameter("cancelMuid");
		String cancelReason = (String) request.getParameter("cancelReason");
		String cancelState = (String) request.getParameter("cancelState");
		String stateReason = (String) request.getParameter("stateReason");
		
		LentCancel lc = new LentCancel(cancelMuid, cancelReason, cancelState, stateReason);
		
		int updateResult = new LentService().updateLentCancel(lc);
		
		if(cancelState.equals("YES")) {
			if(updateResult>0) {
				int deleteResult = new LentService().deleteLentBike(cancelMuid);
				if(deleteResult>0) {
					response.sendRedirect(request.getContextPath());
				}
			}
		} else {
			response.sendRedirect(request.getContextPath());
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