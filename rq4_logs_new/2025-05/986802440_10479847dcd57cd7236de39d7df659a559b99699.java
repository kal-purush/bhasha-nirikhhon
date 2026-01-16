package com.ceos.spring_vote_21st.security.auth.web.dto;

import com.ceos.spring_vote_21st.member.domain.CeosPosition;
import com.ceos.spring_vote_21st.member.domain.CeosTeam;
import com.ceos.spring_vote_21st.member.domain.Member;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class SignUpDTO {
    private String name;

    private String username;

    private String password;

    private String email;

    private CeosPosition position;

    private CeosTeam team;

}