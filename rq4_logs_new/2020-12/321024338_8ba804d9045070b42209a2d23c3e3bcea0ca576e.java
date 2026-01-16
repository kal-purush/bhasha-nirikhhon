package rw.col.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.library.model.service.LibraryService;
import rw.library.model.vo.BookCase2;
import rw.member.model.vo.Member;
import rw.review.model.service.ReviewService;
import rw.review.model.vo.Book;
import rw.review.model.vo.ReviewCard;

/**
 * Servlet implementation class MainLoadServlet
 */
@WebServlet("/main.rw")
public class MainLoadServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public MainLoadServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//컬렉션 로드
		LibraryService lService = new LibraryService(); 
		//책장 컬렉션 갯수 확인
		int count = lService.countAllLibrary();
		//랜덤 함수 발생
		int rowNum = new Random().nextInt(count) + 1; //library갯수로 랜덤 선택.
		//해당 번호 책장 로드 : 닉네임/id/책장 이름/책 image/책 id
		BookCase2 bc = lService.selectOneLibraryByRow(rowNum);
		//책 로드
		ArrayList<Book> blist = null;
		if(bc!=null) {
			 blist = lService.selectOnePSNLibrary(bc.getBookshelfId());
		}
		
		ReviewService rService = new ReviewService();
		//베스트 리뷰 로드
		ArrayList<ReviewCard> rlist = rService.selectBestReview();
		//리뷰 좋아요 갯수 데이터
		HashMap<String, Integer> reviewLikeList = new HashMap<String, Integer>();
		for(ReviewCard rc : rlist) {
			Integer likeCount = rService.selectOneReviewLike(rc.getReviewId());
			reviewLikeList.put(rc.getReviewId(), likeCount);//리뷰 id를 키로 해서 좋아요 값 매칭.
		}
		
		//index 페이지 호출
		
		RequestDispatcher view = request.getRequestDispatcher("/index.jsp");
		request.setAttribute("bookCol", bc);
		request.setAttribute("bookList", blist);
		request.setAttribute("reviewList", rlist);
		request.setAttribute("likeList", reviewLikeList);
		view.forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}