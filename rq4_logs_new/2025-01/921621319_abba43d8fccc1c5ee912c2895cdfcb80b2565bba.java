package com.metamong.mt.member.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.metamong.mt.member.dto.request.FindMemberRequestDto;
import com.metamong.mt.member.dto.response.LoginResponseDto;
import com.metamong.mt.member.model.Member;
import com.metamong.mt.member.model.Role;
import com.metamong.mt.member.repository.mybatis.MemberMapper;


@Service
public class DefaultMemberService implements MemberService {
    
    @Autowired
    private MemberMapper memberRepository;
    
    @Override
    @Transactional(readOnly = true)
    public LoginResponseDto selectLoginMember(String userid) {
        return memberRepository.selectMember(userid);
    }
    
    @Override
    @Transactional
    public void insertMember(Member member) {
    	if (member.getRole() == Role.ROLE_OWNER) {
            if (member.getBusinessName() == null || member.getBusinessRegistrationNumber() == null) {
                throw new IllegalArgumentException("업주 회원가입 시 사업체명과 사업자등록번호는 필수입니다.");
            }
        }
        memberRepository.insertMember(member);
    }

 

	@Override
	@Transactional(readOnly = true)
	public Member selectMemberEntity(String userId) {
    	Member  member = memberRepository.selectMemberEntity(userId);

        if (member == null) {
        	throw new RuntimeException("사용자를 찾을 수 없습니다.");
        } 
        
		return member;
	}
	   // Refresh Token을 DB에 저장하는 메서드 구현

	@Override
	@Transactional 
	public void storeRefreshToken(Member member) {

        // 사용자가 존재하는 경우, refreshToken을 저장
        if (member != null) {
        	member.setRefreshToken(member.getRefreshToken());  
            memberRepository.updateMember(member);  
        } else {
            throw new RuntimeException("사용자를 찾을 수 없습니다.");
        }
		
	}
	@Override
    @Transactional
    public void removeRefreshToken(String userId) {
        Member member = memberRepository.selectMemberEntity(userId);
        if (member != null) {
            member.setRefreshToken(null);  // 리프레시 토큰을 null로 설정
            memberRepository.updateMember(member);   // 리프레시 토큰 null을 DB에 반영
        }
    }
	
	@Override
	public boolean findMember(FindMemberRequestDto request) {	
		boolean requestSuccess = true;
		
		/*
		 * idOrPw가 id인 경우 name, email 데이터만 오고, userid는 null값으로 온다.
		 * idOrPw가 pw인 경우 userid, name, email 데이터 전부 온다.
		 */
		System.out.println("====================================");
		System.out.println("idOrPw: " + request.getIdOrPw());
		System.out.println("userid: " + request.getUserid());
		System.out.println("name: " + request.getName());
		System.out.println("email: " + request.getEmail());
		// 이메일 인증 요청 성공 시
		if(1 == 1){
			requestSuccess = true;	
		}
		// 이메일 인증 요청 실패 시
		else {
			requestSuccess = false;
		}
		
		return requestSuccess;
	}
    
}