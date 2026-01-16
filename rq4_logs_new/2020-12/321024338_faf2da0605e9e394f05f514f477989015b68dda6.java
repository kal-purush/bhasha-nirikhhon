package rw.col.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rw.col.model.service.CollectionService;
import rw.col.model.vo.BookshelfCollection;
import rw.col.model.vo.CollectionData;
import rw.col.model.vo.LibraryCollection;
import rw.col.model.vo.OtherBookcase;
import rw.col.model.vo.ReviewCollection;
import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;
import rw.review.model.service.ReviewService;
import rw.review.model.vo.ReviewCard;

/**
 * Servlet implementation class CollectionLoadServlet
 */
@WebServlet("/myCollection.rw")
public class CollectionLoadServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CollectionLoadServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//처음 받는 입력값
		String memberId = request.getParameter("libraryOwner");
		
		//연결하려면 해당 서재 멤버 정보 필요
		Member owner = new MemberService().selectOneMemberId(memberId);
		
		//가져와야될 정보 6개 리뷰,서재,책장, 각각의 페이지네비
		//collectionData 생성 : 리스트+페이지네비 * 3
		
		CollectionService colService = new CollectionService();
		
		//1) 리뷰 데이터
		int reviewCurrentPage;
		
		if(request.getParameter("reviewCurrentPage")==null) {
			reviewCurrentPage = 1;
		}else {
			reviewCurrentPage = Integer.parseInt(request.getParameter("reviewCurrentPage"));
		}
		
		//쿼리문 수정 필요. - 리뷰컬렉션
		CollectionData<ReviewCard, ReviewCollection> cdRR = colService.selectReviewCollection(reviewCurrentPage, owner.getMemberNo());
		int rcTotalCount = colService.rcTotalCount(owner.getMemberNo());
		HashMap<String, Integer> reviewLikeList = new HashMap<String, Integer>();
		ArrayList<ReviewCard> rcList = cdRR.getList();
		for(ReviewCard rc : rcList) {
			Integer likeCount = new ReviewService().selectOneReviewLike(rc.getReviewId());
			reviewLikeList.put(rc.getReviewId(), likeCount);
		}
		
		//2) 서재 데이터
		int libraryCurrentPage;
		
		if(request.getParameter("libraryCurrentPage")==null) {
			libraryCurrentPage = 1;
		}else {
			libraryCurrentPage = Integer.parseInt(request.getParameter("libraryCurrentPage"));
		}
		
		CollectionData<Member, LibraryCollection> cdML = colService.selectLibraryCollection(libraryCurrentPage,owner.getMemberNo());
		int lcTotalCount = colService.lcTotalCount(owner.getMemberNo()); //게시물 총 갯수
		
		//3) 책장 데이터 
		int bookshelfCurrentPage;
		
		if(request.getParameter("bookshelfCurrentPage")==null) {
			bookshelfCurrentPage = 1;
		}else {
			bookshelfCurrentPage = Integer.parseInt(request.getParameter("bookshelfCurrentPage"));
		}
		
		CollectionData<OtherBookcase,BookshelfCollection> cdBB = colService.selectBookshelfCollection(bookshelfCurrentPage, owner.getMemberNo());
		int bsTotalCount = colService.bsTotalCount(owner.getMemberNo());
		
		//데이터 전송
		RequestDispatcher view = request.getRequestDispatcher("/views/library/collection.jsp");
		request.setAttribute("owner", owner);
		request.setAttribute("review", cdRR);
		request.setAttribute("rcTotalCount", rcTotalCount);
		request.setAttribute("reviewLikeList", reviewLikeList);
		request.setAttribute("library", cdML);
		request.setAttribute("lcTotalCount", lcTotalCount);
		request.setAttribute("bookshelf", cdBB);
		request.setAttribute("bsTotalCount", bsTotalCount);
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