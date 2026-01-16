package com.hanyahunya.gitRemind.unit.member.service;

import com.hanyahunya.gitRemind.member.dto.JoinRequestDto;
import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.member.repository.MemberRepository;
import com.hanyahunya.gitRemind.member.service.MemberService;
import com.hanyahunya.gitRemind.member.service.MemberServiceImpl;
import com.hanyahunya.gitRemind.token.service.TokenService;
import com.hanyahunya.gitRemind.util.ResponseDto;
import com.hanyahunya.gitRemind.util.service.EncodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MemberServiceImplTest {
    private MemberRepository memberRepository;
    private TokenService tokenService;
    private EncodeService encodeService;
    private MemberService memberService;

    @BeforeEach
     void setUp() {
        memberRepository = Mockito.mock(MemberRepository.class);
        tokenService = Mockito.mock(TokenService.class);
        encodeService = Mockito.mock(EncodeService.class);

        memberService = new MemberServiceImpl(memberRepository, tokenService, encodeService);
    }

    @Test
    void join() {
        // given
        JoinRequestDto requestDto = new JoinRequestDto();
        requestDto.setLoginId("test");
        requestDto.setPassword("rawPassword");
        requestDto.setEmail("test@example.com");
        requestDto.setCountry("KR");

        // rawPasswordっていう文字列を受けたら、encodedPasswordを返す
        when(encodeService.encode("rawPassword")).thenReturn("encodedPassword");

        // どのMemberオブジェクトが入っても、trueを返す
        when(memberRepository.saveMember(any(Member.class))).thenReturn(true);

        // when
        ResponseDto<Void> response = memberService.join(requestDto);

        // then
        assertTrue(response.isSuccess()); //ロジックが成功したら、response.isSuccess()を返すのか
        assertEquals("会員登録成功", response.getMessage());

        //　encodeServiceのencodeメソッドに入った文字列がrawPasswordなのか。このメソッドが一回呼ばれたか
        verify(encodeService).encode("rawPassword");
        //　memberRepositoryのsaveMemberメソッドに入ったMemberオブジェクトが正しく入ったのか
        verify(memberRepository).saveMember(argThat(member ->
                member.getLoginId().equals("test") &&
                        member.getPassword().equals("encodedPassword") &&
                        member.getEmail().equals("test@example.com") &&
                        member.getCountry().equals("KR") &&
                        member.getMemberId() != null
        ));
    }

    @Test
    void login() {
    }

    @Test
    void getInfo() {
    }

    @Test
    void deleteMember() {
    }

    @Test
    void updateMember() {
    }
}