package com.web.controller;

import java.security.Principal;
import java.util.HashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.web.domain.Board;
import com.web.domain.Member;
import com.web.domain.Reply;
import com.web.service.BoardService;
import com.web.service.MemberService;
import com.web.service.ReplyService;

@RequestMapping("board")
@Controller
public class BoardController {
	
	@Autowired
	private BoardService boardService;
	
	@Autowired
	private ReplyService replyService;

	@Autowired
	private MemberService memberService;
	

	
	/***
	 * 	게시판 Controller
	 ***/
	//----------------------------------------------------------

	// 거래 게시판 
	@GetMapping("/board")
	public String board(Model model, Board board,
	        @PageableDefault(page = 0, size = 10, sort = "boardSeq", direction = Sort.Direction.DESC) Pageable pageable,
	        String searchCode, String searchKeyword, Long boardChoice,
	        Principal principal, ModelMap modelMap) {
	    boardService.board(model, board, pageable, searchCode, searchKeyword, boardChoice);
	    boardService.boardNotice(model); 

	    Member member = memberService.getCurrentUser(principal);
	    if (member != null) {
	        modelMap.addAttribute("member", member);
	        return "board/board";
	    } else {
	        // 로그인되지 않은 경우
	        return "redirect:/login"; // 로그인 페이지로 리다이렉트 또는 다른 처리
	    }
	}
	
	//----------------------------------------------------------
	
	// 거래 게시판 -> 상세 페이지  
	@GetMapping("/board_view")
	public String board_view(Model model, Long boardSeq, Principal principal, ModelMap modelMap, RedirectAttributes ra) {
	    if (principal != null) {
	        String username = principal.getName();
	        Member member = memberService.getMemberInfo(username);

	        // seq 값으로 조회하여 모델에 추가 후 seq값 없을 때 조회 불가능한 에러 잡기
	        int a = boardService.board_view(model, boardSeq);

	        if (a == 1) { // seq값으로 조회 성공
	            // 게시판 정보 불러올 때 댓글정보도 같이 불러오기
	            replyService.replyList(model, boardSeq);
	            return "board/board_view";
	        } else { // seq 값으로 조회 실패
	            // 게시판 첫 페이지로 이동
	            ra.addAttribute("boardChoice", 0);
	            return "redirect:board";
	        }
	    }
	    return "login";
	}
	//----------------------------------------------------------
	
	 @GetMapping("/board_write")
	    public String boardWrite(String username, ModelMap modelMap) {
	        if (username != null) {
	            Member member = memberService.getMemberInfo(username);
	            modelMap.addAttribute("member", member);
	            return "board/board_write";
	        }
	        return "login";
	    }
	
	// 작성자가 작성한 게시글 등록
    @PostMapping("/board_write_form")
    public String boardWriteForm(MultipartHttpServletRequest mul, Long boardChoice, RedirectAttributes ra) {
        boardService.board_write(mul);
        ra.addAttribute("boardChoice", boardChoice);
        return "redirect:/board";
    }
	//----------------------------------------------------------

	// **게시글 수정 시 정보 데아터 & seq 저장 + 페이징처리 
	@GetMapping("/board_update")
	public String board_update_form(Model model,Long boardSeq, String username, ModelMap modelMap){
		Member member = memberService.getMemberInfo(username);
		modelMap.addAttribute("member", member);
		boardService.getBoard(boardSeq,model);
		return "board/board_update";
	}
	// **게시글 수정
	@PostMapping("/boardUpdate")
	public String boardUpdate(MultipartHttpServletRequest mul,RedirectAttributes ra) {
		boardService.board_update(mul);
		ra.addAttribute("boardChoice",mul.getParameter("boardChoice"));
		return "redirect:board";
	}
	
	//----------------------------------------------------------

	// 게시글 삭제 
	@GetMapping("/board_delete")
	public String board_delete(Board board,Long boardChoice, RedirectAttributes ra) {
		boardService.board_delete(board);
		ra.addAttribute("boardChoice",boardChoice);
		return "redirect:board";
	}
	
	/***
	 * 	댓글 Controller
	 ***/
	//----------------------------------------------------------

	// 댓글 등록 
	@PostMapping("/replyForm")
	public String replyWrite(Reply reply, Long boardSeq, RedirectAttributes ra,Long boardChoice, String page) {
		replyService.replyWrite(reply, boardSeq);
		ra.addAttribute("boardSeq", reply.getBoardSeq());
		ra.addAttribute("boardChoice", boardChoice);
		ra.addAttribute("page", page);
		return "redirect:board_view";
	}
	
	//----------------------------------------------------------

	// 댓글 수정 폼 띄우기
	@GetMapping("/replyModify")
	public String replyModify(Long replySeq, Model model) {
		replyService.replyModify(replySeq, model);
		return "board/replyModify";
	}

	//----------------------------------------------------------
	// 댓글 수정
	@PostMapping("/replyModifyForm")
	public String replyModifyForm(Reply reply, RedirectAttributes ra) {
		replyService.replyModify2(reply);
		ra.addAttribute("replySeq", reply.getReplySeq());
		return "redirect:replyModify";
	}
	//----------------------------------------------------------
	// 댓글 삭제 
	@GetMapping("/replyDelete")
	public String replyDelete(Long replySeq, RedirectAttributes ra,Long boardChoice, String page) {
		replyService.replyDelete(replySeq, ra);
		ra.addAttribute("boardChoice", boardChoice);
		ra.addAttribute("page", page);
		return "redirect:board_view";
	}

	/***
	 * 	게시판 카테고리 선택 Controller
	 ***/

	
	//------------------------------------------------------------
	// 내가 쓴 글 
	@GetMapping("/myBoardList")
	public String myBoardList(Model model, Board board,
			@PageableDefault(page = 0, size = 3, sort = "boardSeq", direction = Sort.Direction.DESC) Pageable pageable,
			String username, ModelMap modelMap) {
		Member member = memberService.getMemberInfo(username);
		modelMap.addAttribute("member", member);
		System.out.println("myBoardList--controller" + board);
		boardService.myBoardList(model, board, pageable, member);

		return "board/myBoardList";
	}
	
	
}