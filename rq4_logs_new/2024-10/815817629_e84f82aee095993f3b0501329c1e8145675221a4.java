package jpabook.jpashop.domain.dto;

import com.querydsl.core.annotations.QueryProjection;
import lombok.Data;
import org.springframework.data.jpa.repository.Query;

@Data
public class MemberTeamDto {

    private Long memberId;

    private String userName;

    private int age;

    private Long teamId;

    private String teamName;

    @QueryProjection
    public MemberTeamDto(Long memberId, String userName, int age, Long teamId, String teamName) {
        this.memberId = memberId;
        this.userName = userName;
        this.age = age;
        this.teamId = teamId;
        this.teamName = teamName;
    }

}