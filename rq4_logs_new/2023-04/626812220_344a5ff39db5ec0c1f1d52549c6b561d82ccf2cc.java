package com.billi.frontcontroller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.model.MembersService;
import com.billi.model.ReviewsService;
import com.billi.vo.MembersVO;
import com.billi.vo.ReviewsVO;

public class ReviewwriteController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		String page="";
		String method = (String)data.get("method");
		HttpServletRequest request = (HttpServletRequest)data.get("request");
		
		if(method.equals("GET")) {
			page = "reviewwrite.jsp";
		}else { //db에 유저 정보 저장
			ReviewsVO reviews = makeReviews(request);
			ReviewsService service = new ReviewsService();
			int result = service.reviewWrite(reviews);
			String path = request.getContextPath();
			
			page = "redirect:"+ path + "/board/boarddetail.do?num=" + request.getParameter("board_id");
		}
		return page;
	}

	private ReviewsVO makeReviews(HttpServletRequest request) {
		String review_content = request.getParameter("review_content");
		String review_writer = request.getParameter("review_writer");
		double rating = Double.parseDouble(request.getParameter("rating"));
		int board_id = Integer.parseInt(request.getParameter("board_id"));
		
		ReviewsVO reviews = new ReviewsVO();
		reviews.setReview_content(review_content);
		reviews.setReview_writer(review_writer);
		reviews.setRating(rating);
		reviews.setBoard_id(board_id);
		
		return reviews;
	}

}