package com.boot.DAO;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.boot.DTO.MovietbDTO;
import com.boot.DTO.ReviewDTO;
import com.boot.DTO.WatchedMovieDTO;

@Mapper
public interface MovieStoryDAO 
{	
	// 본영화+관람편 조회
	public List<WatchedMovieDTO> getWatchedMovies(@Param("uuid") String uuid, @Param("pageSize") int pageSize, @Param("offset") int offset);
	// 본영화 영화 갯수 조회
	public int getTotalCountWatchedMovies(@Param("uuid") String uuid);
	// 좋아요한 영화 목록 조회
	public List<MovietbDTO> getlikedMovies(@Param("uuid") String uuid, @Param("pageSize") int pageSize, @Param("offset") int offset);
	// 좋아요한 영화 갯수 조회
	public int getTotalCountlikedMovies(@Param("uuid") String uuid);
	// 관람평 목록 조회
	public List<ReviewDTO> getUserReviews(@Param("uuid") String uuid, @Param("pageSize") int pageSize, @Param("offset") int offset);
	// 관람평 갯수 조회
	public int getTotalCountUserReviews(@Param("uuid") String uuid);
	
}


