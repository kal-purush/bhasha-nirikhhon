package rw.review.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.simple.JSONObject;

import rw.member.model.vo.Member;
import rw.review.model.service.ReviewService;

/**
 * Servlet implementation class ReviewLikeUpdateServlet
 */
@WebServlet("/reviewLike.rw")
public class ReviewLikeUpdateServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ReviewLikeUpdateServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String reviewId = request.getParameter("reviewId");
		
		HttpSession session = request.getSession();
		Member m = (Member)session.getAttribute("member");
		String memberNo = m.getMemberNo();
		
		ReviewService rService = new ReviewService();
		//일단 있는지 확인하고 있으면 값을 가져올것
		char resultYN = rService.existsReviewLike(reviewId,memberNo);
		
		int result = 0; // DB처리 결과
		char yn = 'N'; //현재 DB에 넣은 문자
		//값이 있으면 Y는 N으로, N은 Y로 하고 좋아요 여부랑 같이 데이터 반환해줄 것.
		if(resultYN=='F') { //없으면 추가
			result = rService.insertReviewLike(reviewId,memberNo);
			if(result>0) {
				yn = 'Y';
			}
		}else if(resultYN=='Y'){ //있으면 N 또는 Y로 수정
			result = rService.updateReviewLike('N',reviewId,memberNo);
			if(result>0) {
				yn = 'N';
			}
		}else if(resultYN=='N') {
			result = rService.updateReviewLike('Y',reviewId,memberNo);
			if(result>0) {
				yn = 'Y';
			}
		}
		if(result<=0) {
			System.out.println("상태를 변경할 수 없습니다.");
		}
		
		//총 카운트 가져오기
		int likeCount = rService.selectOneReviewLike(reviewId); //Y만 가져오기
		
		JSONObject object = new JSONObject();
		
		object.put("count", likeCount);
		object.put("yn", Character.toString(yn));
		
		System.out.println(object);
		
		response.setContentType("application/json");
		response.setCharacterEncoding("UTF-8");
		response.getWriter().print(object); // 출력구문
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}