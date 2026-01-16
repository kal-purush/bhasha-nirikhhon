package com.geekmovie.onelinereview;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.geekmovie.onelinereview.service.OneLineReviewLikeService;
import com.geekmovie.onelinereview.service.OneLineReviewService;
import com.geekmovie.onelinereview.vo.OneLineReviewPageVo;
import com.geekmovie.onelinereview.vo.OneLineReviewVo;



@RestController
public class OneLineReviewLikeControllerApi {

	@Autowired
	OneLineReviewLikeService oneLineReviewLikeService;
	@Autowired
	OneLineReviewService oneLineReviewService;
	
	@RequestMapping(value = "/oneLineReview/Like/{olrid}", method = RequestMethod.POST)
	@ResponseBody
	public int oneLineReviewLikePOST(@RequestBody Map<String, Object> map, @PathVariable("olrid") int olrid) {		//좋아요 버튼 누르면 반영  #{userId}, #{mode}
		map.put("oneLineReviewId", olrid);
		System.out.println(map);
		int rs = oneLineReviewLikeService.Add(map);		//input #{userId}, #{oneLineReviewId}, #{mode}
		oneLineReviewService.UpdateLike(map);		//한줄평 db 좋아요 수 업데이트
		return rs;
	}
	
	
}