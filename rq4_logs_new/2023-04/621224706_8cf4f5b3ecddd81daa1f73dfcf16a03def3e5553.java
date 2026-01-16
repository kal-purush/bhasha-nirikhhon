package JPABook.JPAShop.api;

import JPABook.JPAShop.domain.Member;
import JPABook.JPAShop.service.MemberService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor //final 로 선언된 의존성 변수의 주입을 생성자 의존성 주입을 통해 구현해 준다.
public class MemberApiController {
    private final MemberService memberService;

    // 회원생성 ======================================================================

    /**
     * COMMENT
     * 해당 v1 의 문제점
     * - @RequestBody Member
     *    : Member 엔티티의 생성시 필수값을 설정하고 싶은 요구사항의 경우 Member 엔티티 자체를 수정해 주어야 함
     *      -> 위의 결과는 추가적으로 다른 진입점의 Member 엔티티 생성의 경우에도 위의 요구사항을 반영한 필수값이
     *         정해져 있는 엔티티 밖에 사용하지 못하게 된다는 갇힌 확장성을 가진 엔티티를 가지게 됨
     *    : 엔티티에 API 검증을 위한 로직이 들어가게 된다.(프레젠테이션에 필요한 로직이 엔티티에 들어가게 되는 것)
     *    : 엔티티가 변경되면 API 스펙이 변하게 된다. (name -> username)
     *    : 실무에서는 회원 엔티티를 위한 다양한 API 가 요구 되게 되는데, 한 엔티티에 각각을 대응하기 위한 로직을 담기는 어렵다.
     * - Response by return Member
     *    : 반환 값을 Member Entity 자체로 매핑되어 나가게 될 경우
     *      Member 엔티티의 모든 필드 값이 노출되어 나가게 된다.(eg. password, 주소 등)
     *    : 실무에서는 엔티티를 API 스펙에 노출하게 되는 것이 대단히 위험하다
     */
    @PostMapping("api/v1/members")
    public Member createMember(@RequestBody @Valid Member member) {
        memberService.join(member);
        return member;
    }

    /**
     * v2 가 해결한 점
     * - req, res 를 한번 매핑해주는 계층을 둠으로서, 엔티티와 프레젠테이션 을 위한 로직을 분리할 수 있다.
     * - 엔티티의 수정이 API 수정을 요구하지 않게 된다.
     * - 엔티티의 필드명이 수정되더라도, API 스펙이 깨져서 확인 가능하게 되는 것이 아니라, req.getName() 에서 컴파일 에러를 통해
     * 확인이 가능해짐으로써 오류의 확인 시점을 앞 당길 수 있게 된다.
     */
    @PostMapping("api/v2/members")
    public CreateMemberResponse saveMemberV2(@RequestBody @Valid CreateMemberRequest request) {
        Member member = new Member();
        member.setName(request.getName());

        Long id = memberService.join(member);

        return new CreateMemberResponse(id);
    }

    @Data // name 이 NotEmpty 이기를 요구하는 상황의 프레젠테이션 비즈니스 제약 조건을 가진 경우 별로의 대응이 가능한 구현
    static class CreateMemberRequest {
        @NotEmpty
        private String name;
    }


    @Data
    private class CreateMemberResponse {
        private Long id;

        public CreateMemberResponse(Long id) {
            this.id = id;
        }
    }

    // 회원수정 ======================================================================
    @PatchMapping("api/v2/members/{id}")
    public UpdateMemberResponse updateMember(
            @PathVariable("id") Long id,
            @RequestBody UpdateMemberRequest request) {

        memberService.update(id, request.getName());
        Member updateMember = memberService.findOne(id);

        return new UpdateMemberResponse(updateMember.getId(), updateMember.getName());
    }

    @Data
    static class UpdateMemberRequest {
        private String name;
    }

    @Data
    @AllArgsConstructor
    static class UpdateMemberResponse {
        private Long id;
        private String afterName;
    }

    // 회원수정 ======================================================================

    /**
     * 해당 조회 v1 의 문제점
     * - 엔티티의 모든 값이 노출 된다.
     * - 특정 API 스펙을 위한 @JsonIgnore 등의 검증 로직이 엔티티에 추가되게 된다.
     * - 응닶 json 값의 형태를 커스텀하지 못한다. (eg. data: []...)
     */
    @GetMapping("api/v1/members")
    public List<Member> membersV1() {
        return memberService.findMemberAll();
    }

    @GetMapping("api/v2/members")
    public MembersResponse membersV2() {
        List<Member> findMembers = memberService.findMemberAll();
        List<MemberDto> collectMembers = findMembers.stream()
                .map(m -> new MemberDto(m.getName()))
                .collect(Collectors.toList());

        return new MembersResponse(collectMembers.size(), collectMembers);
    }

    //todo: 왜 static 으로 DTO 를 구현해야 하는지
    @Data
    @AllArgsConstructor
    private static class MembersResponse<T> {
        private int count;
        private T data;
    }

    @Data
    @AllArgsConstructor
    private static class MemberDto {
        private String name;
    }

}