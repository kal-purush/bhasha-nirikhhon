package refooding.api.domain.member.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import refooding.api.domain.member.dto.MemberRequest;
import refooding.api.domain.member.dto.MemberResponse;
import refooding.api.domain.member.entity.Member;
import refooding.api.domain.member.service.MemberService;
import refooding.api.domain.recipe.dto.RecipeResponse;

import java.util.List;
import java.util.stream.Collectors;

@Tag(name = "테스트용 멤버 CRUD API")
@RestController
@RequestMapping("/members")
@RequiredArgsConstructor
public class MemberController {

    private final MemberService memberService;

    @PostMapping
    @Operation(
            summary = "회원 가입",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "회원 가입 성공",
                            content = @Content(schema = @Schema(implementation = MemberResponse.class))
                    )
            }
    )
    public ResponseEntity<Long> join(@RequestBody MemberRequest memberRequest) {
        Long memberId = memberService.join(memberRequest);
        return ResponseEntity.ok(memberId);
    }


    @GetMapping
    @Operation(
            summary = "전체 회원 목록 조회 - 탈퇴한 회원 제외",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "탈퇴한 회원 제외 전체 회원 목록 조회 성공",
                            content = @Content(schema = @Schema(implementation = MemberResponse.class))
                    )
            }
    )
    public ResponseEntity<List<MemberResponse>> findAllMembers() {
        List<MemberResponse> response = memberService.findMembers().stream()
                .map(member -> MemberResponse.builder()
                        .id(member.getId())
                        .name(member.getName())
                        .build())
                .toList();
        return ResponseEntity.ok(response);
    }

    @GetMapping("/all")
    @Operation(
            summary = "전체 회원 목록 조회 - 탈퇴한 회원 포함",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "탈퇴한 회원 포함 전체 회원 목록 조회 성공",
                            content = @Content(schema = @Schema(implementation = MemberResponse.class))
                    )
            }
    )
    public ResponseEntity<List<MemberResponse>> findAllMembersIncludingDeleted() {
        List<MemberResponse> members = memberService.findMembersIncludingDeleted().stream()
                .map(member -> MemberResponse.builder()
                        .id(member.getId())
                        .name(member.getName())
                        .build())
                .toList();
        return ResponseEntity.ok(members);
    }

    @GetMapping("/{memberId}")
    @Operation(
            summary = "회원 상세 조회",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "회원 상세 조회 성공",
                            content = @Content(schema = @Schema(implementation = MemberResponse.class))
                    )
            }
    )
    public ResponseEntity<MemberResponse> findOne(@PathVariable Long memberId) {
        MemberResponse response = memberService.findOne(memberId);
        return ResponseEntity.ok(response);
    }

    @PatchMapping("/{memberId}")
    @Operation(
            summary = "회원 이름 수정",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "회원 이름 수정 성공"
                    ),
                    @ApiResponse(
                            responseCode = "404",
                            description = "회원이 존재하지 않음"
                    )
            }
    )
    public ResponseEntity<Void> updateMemberName(@PathVariable Long memberId, @RequestBody MemberRequest memberRequest) {
        memberService.updateMemberName(memberId, memberRequest.getName());
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{memberId}")
    @Operation(
            summary = "회원 탈퇴",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "회원 탈퇴 성공"
                    ),
                    @ApiResponse(
                            responseCode = "404",
                            description = "회원이 존재하지 않음"
                    )
            }
    )
    public ResponseEntity<Void> deleteMember(@PathVariable Long memberId) {
        memberService.deleteMember(memberId);
        return ResponseEntity.ok().build();
    }
}