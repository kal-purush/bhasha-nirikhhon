package com.ceos.spring_vote_21st.member.service;

import com.ceos.spring_vote_21st.global.error.CustomException;
import com.ceos.spring_vote_21st.global.error.ErrorCode;
import com.ceos.spring_vote_21st.member.domain.Member;
import com.ceos.spring_vote_21st.member.repository.MemberRepository;
import com.ceos.spring_vote_21st.member.web.dto.MemberCreateRequestDTO;
import com.ceos.spring_vote_21st.member.web.dto.MemberResponseDTO;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Transactional(readOnly = false)
@RequiredArgsConstructor
@Service
public class MemberService {

    private final MemberRepository memberRepository;
    private final PasswordEncoder passwordEncoder;

    /** CREATE */
    @Transactional
    public Long createMember(MemberCreateRequestDTO dto) {
        Member entity = dto.toEntity();
        if (memberRepository.existsByUsername(entity.getUsername())) {
            throw new CustomException(ErrorCode.USERNAME_ALREADY_EXISTS);
        }
        if (memberRepository.existsByEmail(entity.getEmail())) {
            throw new CustomException(ErrorCode.EMAIL_ALREADY_EXISTS);
        }
        return memberRepository.save(entity).getId();
    }

    /** READ (단건 조회) */
    public MemberResponseDTO getMember(Long id) {
        Member member = memberRepository.findById(id)
                .orElseThrow(() -> new CustomException(ErrorCode.MEMBER_NOT_EXISTS));

        return MemberResponseDTO.from(member);
    }

    /** READ (전체 조회) */
    public List<MemberResponseDTO> getAllMembers() {
        List<Member> findMembers = memberRepository.findAll();

        List<MemberResponseDTO> dtos = findMembers.stream()
                .map(member -> MemberResponseDTO.from(member))
                .collect(Collectors.toList());
        return dtos;
    }

    /** UPDATE */
/*
    @Transactional
    public Member updateMember(Long id, Member updateData) {
        Member member = memberRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("해당 ID의 회원이 존재하지 않습니다."));

        member = Member.builder()
                .id(member.getId())
                .name(updateData.getName())
                .username(updateData.getUsername())
                .password(updateData.getPassword())
                .email(updateData.getEmail())
                .position(updateData.getPosition())
                .team(updateData.getTeam())
                .build();

        return memberRepository.save(member).toDTO;
    }
*/

    /** DELETE */
    @Transactional
    public void deleteMember(Long id) {
        if (!memberRepository.existsById(id)) {
            throw new CustomException(ErrorCode.MEMBER_NOT_EXISTS);
        }
        memberRepository.deleteById(id);
    }
}