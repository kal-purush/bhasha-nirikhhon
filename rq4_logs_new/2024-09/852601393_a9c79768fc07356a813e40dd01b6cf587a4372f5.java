package com.boot.DAO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.boot.DTO.ReviewDTO;

@Mapper
public interface ReviewDAO 
{	
	// 리뷰 가져오는 쿼리
	public ArrayList<ReviewDTO> selectReview(HashMap<String, String>param); 
	
	// 리뷰+페이징 가져오는 쿼리
	public ArrayList<ReviewDTO> selectReviewPaging(ReviewDTO reviewdto); 
	
	// 리뷰 입력
	public void insertReview(HashMap<String, String>param);
	
	// 리뷰 수정
	public void updateReview(HashMap<String, String>param);
	
	// 리뷰 삭제
	public void deleteReview(HashMap<String, String> param);
	
	// 리뷰 중복 확인
	public int findMovieno(HashMap<String, String>param);
	
	// 해당 영화 리뷰 갯수
//	public int countReview(HashMap<String, String>param);
	public int countReview(ReviewDTO reviewdto);
	
	// 해당 영화 리뷰 갯수
	public Integer countstar(HashMap<String, String>param);
	

	
}


