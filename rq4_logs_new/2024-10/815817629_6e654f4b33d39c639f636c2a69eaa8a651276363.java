package jpabook.jpashop.controller;

import jpabook.jpashop.domain.dto.MemberSearchCondition;
import jpabook.jpashop.domain.dto.MemberTeamDto;
import jpabook.jpashop.repository.MemberJpaQueryDslRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class QueryDslMemberController {

    private final MemberJpaQueryDslRepository memberJpaQueryDslRepository;

    @GetMapping("/v1/query-dsl-members")
    public List<MemberTeamDto> searchQueryDslMemberV1(MemberSearchCondition cond) {
        return memberJpaQueryDslRepository.search(cond);
    }
}