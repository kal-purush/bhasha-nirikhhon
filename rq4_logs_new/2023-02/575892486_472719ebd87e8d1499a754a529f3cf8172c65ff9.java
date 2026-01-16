package com.corn.market.member.service;

import com.corn.market.member.dao.MemberDao;
import com.corn.market.member.domain.LoginMember;
import com.corn.market.member.domain.Member;
import org.springframework.stereotype.Service;

@Service
public class MemberService {

    private final MemberDao dao;
    public MemberService(MemberDao dao) {
        this.dao = dao;
    }

    //회원가입
    public void memberSignup(Member member) throws Exception{
        dao.memberSignup(member);
    }

    //아이디 중복 검사
    public int idCheck(String user_id) throws Exception{
        return  dao.idCheck(user_id); //중복이면 1 아니면 0
    }
    //닉네임 중복 검사
    public int nicknameCheck(String nickname) throws Exception{
        return dao.nicknameCheck(nickname); //중복이면 1 아니면 0
    }

    //로그인 확인 비번+아이디 일치 확인
    public int checkLogin(LoginMember member) throws Exception {
        return dao.checkLogin(member); //아이디 비밀번호 일치하면 1, 아니면 0
    }

}