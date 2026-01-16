package gift.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import gift.model.service.GiftService;
import gift.model.vo.Gift;
import lent.model.service.LentService;
import lent.model.vo.LentBike;
import lent.model.vo.PurchaseTicket;
import shop.model.vo.Shop;

/**
 * Servlet implementation class LentConfirm
 */
@WebServlet("/gift/GiftConfirm")
public class GiftConfirm extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GiftConfirm() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("컨펌 서블릿");
		String sendMemId = request.getParameter("sendMemId");
		String merchantUid = request.getParameter("merchantUid");
		int methodNum = Integer.parseInt(request.getParameter("methodNum"));
		String bikeId = request.getParameter("bikeId");		
		String buyDate = request.getParameter("buyDate");
		String returnDate = request.getParameter("returnDate");
		String presentUid = request.getParameter("presentUid");
		System.out.println("넘어오냐 sendmemId 컨펌 서블릿"+sendMemId);
		String buyerId = request.getParameter("buyerId");
		String shopId = request.getParameter("shopId");
		int lentPrice = Integer.parseInt(request.getParameter("lentPrice"));
		
		
			LentBike lb = new LentBike(merchantUid,methodNum,bikeId,buyDate,returnDate,buyerId,shopId,lentPrice);
			Gift gift = new Gift(presentUid,methodNum,sendMemId);
			// DB저장
			new LentService().insertLent(lb);
			new GiftService().insertGift(gift);
			// 저장된 데이터를 가져온다. giftConfirmEnd.jsp에 출력용
			Gift selectGift = new GiftService().selectGift(presentUid);
			System.out.println(gift+"기프트 객체 컨펌 서블릿");
			request.setAttribute("selectGift", selectGift);
			request.getRequestDispatcher("/views/gift/giftConfirmEnd.jsp").forward(request, response);
		 
	
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
}