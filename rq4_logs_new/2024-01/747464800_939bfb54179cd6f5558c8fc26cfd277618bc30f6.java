package com.example.demo.jpa;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.example.demo.vo.MathBookVo;
import com.example.demo.vo.MypageInfo;

public interface MypageInfoRepo extends JpaRepository<MypageInfo, String>{
	@Query(value = "select * from mypageinfo where id =:na ", nativeQuery = true)
	public List<MypageInfo> selectById(@Param(value="na") String id);
}