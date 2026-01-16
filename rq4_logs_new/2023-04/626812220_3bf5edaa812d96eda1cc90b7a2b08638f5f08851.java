package com.billi.frontcontroller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.model.ReviewsService;

public class ReviewdeleteController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		HttpServletRequest request = (HttpServletRequest)data.get("request");
		int review_id = Integer.parseInt(request.getParameter("review_id"));
		
		ReviewsService service = new ReviewsService();
		service.reviewDelete(review_id);
		
		return "redirect:myreview.do";
	}

}