package com.ratseno.board.board.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ratseno.board.board.model.req.ReplyReq;
import com.ratseno.board.board.model.res.BoardListRes;
import com.ratseno.board.board.model.res.ReplyListRes;
import com.ratseno.board.board.service.ReplyService;
import com.ratseno.board.common.annotation.LoginMemberInfo;

@RestController
@RequestMapping("/reply")
public class ReplyController {

	@Autowired
	ReplyService replyService;
	
	@RequestMapping(value="/regist", method=RequestMethod.POST)
	@LoginMemberInfo
	public ResponseEntity<String> replyRegist(HttpServletRequest request, @RequestBody ReplyReq replyReq){
		ResponseEntity<String> entity = null;
		ReplyReq req = replyReq;
		
		try {
			replyService.replyRegist(req);
			entity = new ResponseEntity<String>("SUCCESS", HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
		return entity;
	}
	
	@RequestMapping(value="/list", method=RequestMethod.GET)
	public ResponseEntity<ReplyListRes> replyList(HttpServletRequest request, @RequestParam("board_no")Integer boardNo){
		ResponseEntity<ReplyListRes> entity = null;
		Integer board_no = boardNo;
		
		ReplyListRes res = new ReplyListRes();
		
		try {
			res = replyService.replyList(board_no);
			entity = new ResponseEntity<ReplyListRes>(res, HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<ReplyListRes>(HttpStatus.BAD_REQUEST);
		}
		return entity;
	}
	
	@RequestMapping(value="/update", method=RequestMethod.POST)
	@LoginMemberInfo
	public ResponseEntity<String> modifyReply(HttpServletRequest request, @RequestBody ReplyReq replyReq){
		ResponseEntity<String> entity = null;
		ReplyReq req = replyReq;
		
		try {
			replyService.modifyReply(req);
			entity = new ResponseEntity<String>("SUCCESS", HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
		return entity;
	}
	
	@RequestMapping(value="/delete", method=RequestMethod.POST)
	@LoginMemberInfo
	public ResponseEntity<String> deleteReply(HttpServletRequest request, @RequestParam("reply_no")Integer replyNo){
		ResponseEntity<String> entity = null;
		Integer reply_no = replyNo;
		
		try {
			replyService.deleteReply(reply_no);
			entity = new ResponseEntity<String>("SUCCESS", HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
		return entity;
	}
	
}