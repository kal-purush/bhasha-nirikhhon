package com.boot.DAO;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.boot.DTO.OauthtbDTO;


@Mapper
public interface OauthtbDAO_3
{
	public void oauthInsertUser(OauthtbDTO oauthtbDTO); // 회원 가입
	public int oauthCheckUserid(String userid);  // ID 중복 체크 메소드
	public boolean existsByUuid(String uuid);    // UUID 중복 체크 메소드
    public boolean oauthCheckNewUser(@Param("oauthUserId") String oauthUserId, @Param("registrationId") String registrationId);
    public OauthtbDTO oauthGetUserById(String userid); // ID로 회원 정보 가져오기
}