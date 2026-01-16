package rw.library.controller;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import rw.col.model.service.CollectionService;
import rw.col.model.vo.BookshelfCollection;
import rw.library.model.service.LibraryService;
import rw.library.model.vo.LibraryPageData;
import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;
import rw.review.model.vo.Book;

/**
 * Servlet implementation class myBookCaseSelectAllServlet
 */
@WebServlet("/myBookCase.rw")
public class LibraryBookCaseSelectAllServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LibraryBookCaseSelectAllServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		LibraryService lService = new LibraryService();
		HttpSession session = request.getSession();
		Member member = (Member)session.getAttribute("member"); // 내 정보
		
		int count = 0; // 책장 개수
		Member m = new Member();
		
		String libraryOwner = request.getParameter("libraryOwner");
		if(member!=null) { // 로그인 했다면

			if(member.getMemberId().equals(libraryOwner)) { //// 내 책장
				m = new MemberService().selectOneMemberId(member.getMemberId());  // 내 서재
				
				if(m!=null) { // 세션에 넣어줌
					session.setAttribute("member", m); 
				}
				
				count = lService.countAllBookCase(m.getMemberNo()); // 책장 개수
				
				// 페이징 처리
				int currentPage; // 현재 페이지값을 가지고 있는 변수
				if(request.getParameter("currentPage")==null) {
					currentPage = 1;
				}else {
					currentPage = Integer.parseInt(request.getParameter("currentPage"));
				}
				
				LibraryPageData list = lService.selectAllBookCase(m.getMemberNo(),libraryOwner,currentPage);
				// 최종적으로 list 안에 담긴 데이터 = (책장 + 해당 책장에 담긴 책들) 객체를 담은 BookCase 객체// 배열
				
				ArrayList<Book> listLB = lService.selectLikeBook(m.getMemberNo()); // 좋아요 한 책 리스트
				
				RequestDispatcher view = request.getRequestDispatcher("/views/library/book_case.jsp?libraryOwner="+libraryOwner);
				request.setAttribute("member", m);
				request.setAttribute("count", count);
				request.setAttribute("list", list);
				request.setAttribute("listLB", listLB);
				view.forward(request, response);
				
			}else { /////// 남으 ㅣ책장
				m = new MemberService().selectOneMemberId(libraryOwner); // 남의 서재
				
				count = lService.countOtherAllBookCase(m.getMemberNo()); // 책장 개수
				
				// 페이징 처리
				int currentPage; // 현재 페이지값을 가지고 있는 변수
				if(request.getParameter("currentPage")==null) {
					currentPage = 1;
				}else {
					currentPage = Integer.parseInt(request.getParameter("currentPage"));
				}
				
				LibraryPageData list = lService.selectOtherAllBookCase(m.getMemberNo(),libraryOwner,currentPage);
				// 최종적으로 list 안에 담긴 데이터 = (책장 + 해당 책장에 담긴 책들) 객체를 담은 BookCase 객체// 배열
				
				CollectionService colService = new CollectionService();
				
				ArrayList<BookshelfCollection> bcColList = colService.selectColBookshelf(member.getMemberNo());
				// 내가 스크랩한 책장
				
				// 내 서재 컬렉션 데이터 가져오기 / 남의 서재가 내 컬렉션에 있는지 확인
				boolean result =  colService.existsMyLibCol(member.getMemberNo(),m.getMemberId()); // 세션 No / Owner Id
				
				RequestDispatcher view = request.getRequestDispatcher("/views/library/book_case.jsp?libraryOwner="+libraryOwner);
				request.setAttribute("member", m);
				request.setAttribute("count", count);
				request.setAttribute("list", list);
				request.setAttribute("bcColList", bcColList); // 책장 컬렉션
				request.setAttribute("inMyLibCol", result); // 서재 컬렉션
				view.forward(request, response);
			}
			
			
		}else { // 로그인 안 했다면
			m = new MemberService().selectOneMemberId(libraryOwner); // 남의 서재
			
			count = lService.countOtherAllBookCase(m.getMemberNo()); // 책장 개수
			
			// 페이징 처리
			int currentPage; // 현재 페이지값을 가지고 있는 변수
			if(request.getParameter("currentPage")==null) {
				currentPage = 1;
			}else {
				currentPage = Integer.parseInt(request.getParameter("currentPage"));
			}
			
			LibraryPageData list = lService.selectOtherAllBookCase(m.getMemberNo(),libraryOwner,currentPage);
			// 최종적으로 list 안에 담긴 데이터 = (책장 + 해당 책장에 담긴 책들) 객체를 담은 BookCase 객체// 배열
			
			
			RequestDispatcher view = request.getRequestDispatcher("/views/library/book_case.jsp?libraryOwner="+libraryOwner);
			request.setAttribute("member", m);
			request.setAttribute("count", count);
			request.setAttribute("list", list);
			view.forward(request, response);
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