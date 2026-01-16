package com.geekmovie.free.reply;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.geekmovie.board.service.FreeBoardService;
import com.geekmovie.board.vo.BoardVo;
import com.geekmovie.board.vo.FreeVo;
import com.geekmovie.board.vo.PageVo;
import com.geekmovie.free.reply.service.FreeReplyService;
import com.geekmovie.free.reply.vo.FreeReplyVo;
import com.geekmovie.onelinereview.vo.OneLineReviewPageVo;

@RestController
public class FreeReplyControllerApi {

	@Autowired
	FreeReplyService freeReplyService;
	@Autowired
	OneLineReviewPageVo pageVo;
	@Autowired
	FreeBoardService freeBoardService;
	
	@RequestMapping(value= "/freeboard/{boardId}/reply", method = RequestMethod.POST)          //게시판 userId가 작성한 글 insert 
	@ResponseBody
	public int freeboardReplyPOST(@RequestBody FreeReplyVo freeReplyVo,	//ancestorId, content,writer,depth, orderSerial
			@PathVariable("boardId") int boardId) {
			String NewOrder=null;
			int NewAnsesterId = 0;
			int NewDepth = freeReplyVo.getDepth();
			freeReplyVo.setBoardId(boardId);
			
		if(NewDepth==0) {	// 댓글 바깥에 답장(depth 안들어감)
			NewAnsesterId = freeReplyService.boardAncestorCount(boardId)+1;
			freeReplyVo.setAncestorId(NewAnsesterId);
			NewOrder = "";
		}else{		//댓글 안에 답장(dept 들어감)

			NewAnsesterId = freeReplyVo.getAncestorId();
			freeReplyVo.setAncestorId(NewAnsesterId);
			NewOrder = freeReplyService.boardOrderserialfind(freeReplyVo);		//1만개의 댓글을 수용
			String NewOrderPiece = NewOrder.substring(NewOrder.length()-3);
			NewOrder = NewOrder.substring(0, NewOrder.length()-4) + String.format("%04d", Integer.parseInt(NewOrderPiece)+1);
			

			

		}
			
			
			freeReplyVo.setOrderserial(NewOrder);
		
			System.out.println(freeReplyVo);
		return freeReplyService.insert(freeReplyVo);
	}

	
	@RequestMapping(value= "/freeboard/{boardId}/reply", method = RequestMethod.GET)          //board의 답글 불러오기
	public String freeboardReplyGET(
			@PathVariable("boardId") int boardId, @RequestParam int page) {
			int totalNum = freeReplyService.boardCount(boardId); //전체 댓글수
			

			Map<String, Object> map2 = new HashMap<String, Object>();	  //전체 댓글 개수 구할때 free db에도 추가
			map2.put("seq",  boardId);
			map2.put("reply", totalNum);
			freeBoardService.replyUpdate(map2);
			
			pageVo.PageInfo(totalNum, page);
			int start = pageVo.getNowPageStart();
			int size = pageVo.getOnePage();
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("boardId", boardId);
			map.put("start", start);
			map.put("size", size);
			List<FreeReplyVo> replyList = freeReplyService.boardShow(map);
			JSONObject json = new JSONObject();
			json.put("data", replyList);
			json.put("endPage", pageVo.getEndPage());
			json.put("replyCnt", totalNum);
			String resp = json.toString();
		return resp;
	}
	
	
	@RequestMapping(value = "/freeboard/reply/{replyId}", method = RequestMethod.DELETE)
	public int freeboardReplyDelete(@PathVariable("replyId") int replyId) {
		int rs = freeReplyService.delete(replyId);			//rest api
		return 	rs;
	}
	
	@RequestMapping(value = "/freeboard/reply/{replyId}", method = RequestMethod.PUT)
	@ResponseBody
	public int freeboardReplyPOST(@RequestBody FreeReplyVo freeReplyVo) {
		return 	freeReplyService.update(freeReplyVo);
	}
}