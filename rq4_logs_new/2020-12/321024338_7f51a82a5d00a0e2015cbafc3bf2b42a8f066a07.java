package rw.review.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.member.model.vo.Member;
import rw.review.model.service.ReviewService;

/**
 * Servlet implementation class ReviewLikeInsertServlet
 */
@WebServlet("/reviewLikeAdd.rw")
public class ReviewLikeInsertServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ReviewLikeInsertServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ReviewService rService = new ReviewService();
		HttpSession session = request.getSession();
		Member member = (Member)session.getAttribute("member");
		
		char resultYN = request.getParameter("like").charAt(0);
		String reviewId = request.getParameter("reviewId");
		String memberNo = member.getMemberNo();
		
		char exist = rService.existsReviewLike(reviewId, memberNo); // 디비에 존재 하는지 여부
		System.out.println(exist);
		int result = 0;
		if(exist=='F') {
			result = rService.insertReviewLike(reviewId,memberNo); // 없으면 인서트
		}else {
			result = rService.updateReviewLike(resultYN, reviewId, memberNo); // 있으면 업데이트
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