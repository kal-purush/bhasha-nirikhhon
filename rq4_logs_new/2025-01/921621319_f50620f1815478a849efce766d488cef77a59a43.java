package com.metamong.mt.member.repository.mybatis;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import com.metamong.mt.member.dto.request.OwnerSignRequestDto;
import com.metamong.mt.member.dto.response.LoginResponseDto;
import com.metamong.mt.member.model.Member;

@Repository
@Mapper
public interface IMemberRepository {
	LoginResponseDto selectMember(String userid);
	void insertMember(Member member);
	void updateMember(Member member);
	boolean existsAdmin();
	Member selectMemberEntity(String userid);
}