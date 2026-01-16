package com.geekmovie.board;

import java.util.List;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.geekmovie.board.service.BoardReplyService;
import com.geekmovie.board.vo.ReplyVo;

@RestController
@RequestMapping("/replies")
public class BoardReplyController {
	
	private final BoardReplyService replyService;
	
	@Inject
	public BoardReplyController(BoardReplyService replyService) {
		this.replyService = replyService;
	}
	
	//reply register
	@RequestMapping(value="", method = RequestMethod.POST)
	public ResponseEntity<String> register(@RequestBody ReplyVo replyVo) {
		ResponseEntity<String> entity = null;
		
		try {
			replyService.rCreate(replyVo);
			entity = new ResponseEntity<String>("regSuccess", HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
		
		return entity;
	}

	
	//reply List
	@RequestMapping(value="/all/{seq}", method = RequestMethod.GET) 
	public ResponseEntity<List<ReplyVo>> rList(@PathVariable("seq") int seq) {
		ResponseEntity<List<ReplyVo>> entity = null;
		
		try {
			entity = new ResponseEntity<List<ReplyVo>>(replyService.rList(seq), HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<List<ReplyVo>>(HttpStatus.BAD_REQUEST);
		}
		
		return entity;
	  }
	
	//reply Modify
	@RequestMapping(value="/{reply_seq}", method = {RequestMethod.PUT, RequestMethod.PATCH})
	public ResponseEntity<String> rUpdate(@PathVariable("reply_seq") int reply_seq, @RequestBody ReplyVo replyVo) {
		ResponseEntity<String> entity = null;
		
		try {
			replyVo.setReply_seq(reply_seq);
			replyService.rUpdate(replyVo);
			entity = new ResponseEntity<String>("modSuccess", HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
		
		return entity;
	}
	 
	//reply Delete
	@RequestMapping(value = "/{reply_seq}", method = RequestMethod.DELETE)
	public ResponseEntity<String> rDelete(@PathVariable("reply_seq") int reply_seq) {
		ResponseEntity<String> entity = null;
		
		try {
			replyService.rDelete(reply_seq);
			entity = new ResponseEntity<String>("delSuccess", HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			entity = new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
		return entity;
	}


	
}
