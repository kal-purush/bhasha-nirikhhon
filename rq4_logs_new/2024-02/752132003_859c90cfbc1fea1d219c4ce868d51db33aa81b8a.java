package com.web.entity;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;

import com.web.domain.Role;
import com.web.domain.SocialType;

import lombok.Data;

@Data
public class LoginMemberDTO {
	private Long memberNum;
	private String memberName; //회원 이름
	private String username; 	//회원 id
	private String password;	//회원 pw
	private String email; 		//회원 이메일
	private String phoneNum; 	//회원 전화번호
	private String membership; 	//회원 등급

    @Enumerated(EnumType.STRING)
    private Role role;
    
    @Enumerated(EnumType.STRING)
    private SocialType socialType; // KAKAO, NAVER, GOOGLE  
    private String socialId; // 로그인한 소셜 타입의 식별자 값 (일반 로그인인 경우 null)
    private String refreshToken; // 리프레시 토큰 


}