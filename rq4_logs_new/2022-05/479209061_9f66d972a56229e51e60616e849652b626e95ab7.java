package com.geekmovie.movie;



import java.util.HashMap;
import java.util.Map;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.geekmovie.movie.service.MovieLikeService;



@RestController
public class MovieLikeControllerApi {
	
	@Autowired
	MovieLikeService movieLikeService;
	
	@RequestMapping(value = "/Like/{movieId}", method = RequestMethod.GET)
	public int oneLineReviewLikeGET(@PathVariable("movieId") int movieId,			//해당하는 유저의 좋아요가 있는지 판별
			@RequestParam String userId, 
			@RequestParam int mode ) {		//#{userId}, #{mode}
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("movieId", movieId);
		map.put("userId", userId);
		map.put("mode", mode);
		return movieLikeService.Check(map);
	}
	
	@RequestMapping(value = "/Like/{movieId}", method = RequestMethod.POST)
	@ResponseBody
	public int oneLineReviewLikePOST(@RequestBody Map<String, Object> map, @PathVariable("movieId") int movieId) {		//좋아요 버튼 누르면 반영  #{userId}, #{mode}
		map.put("movieId", movieId);
		return movieLikeService.Add(map);
	}
	
}
	

	


	
	