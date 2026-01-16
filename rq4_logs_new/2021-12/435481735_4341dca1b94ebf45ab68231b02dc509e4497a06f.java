package com.example.spring.demo.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import com.example.spring.demo.vo.Board;

@Mapper
public interface BoardRepository {
	@SuppressWarnings("preview")
	@Select("""
			SELECT *
			FROM board AS B
			WHERE B.id = #{id}
			AND B.delStatus = 0
				""")
	Board getBoardById(@Param("id") int id);
}