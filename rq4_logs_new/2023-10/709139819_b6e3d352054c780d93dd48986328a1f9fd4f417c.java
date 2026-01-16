package com.thanksd.server.controller;

import com.thanksd.server.dto.request.DiaryRequest;
import com.thanksd.server.dto.response.DiaryAllResponse;
import com.thanksd.server.dto.response.DiaryIdResponse;
import com.thanksd.server.dto.response.DiaryResponse;
import com.thanksd.server.dto.response.Response;
import com.thanksd.server.security.auth.LoginUserId;
import com.thanksd.server.service.DiaryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import javax.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Diaries", description = "일기")
@RestController
@RequiredArgsConstructor
@SecurityRequirement(name = "JWT")
@RequestMapping("/diaries")
public class DiaryController {

    private final DiaryService diaryService;

    @Operation(summary = "모든 일기 불러오기")
    @GetMapping
    public Response<Object> findAllDairy(@LoginUserId Long memberId) {
        DiaryAllResponse response = diaryService.findMemberDiaries(memberId);
        return Response.ofSuccess("OK", response);
    }

    @Operation(summary = "특정 일기 작성")
    @PostMapping
    public Response<Object> saveDiary(@LoginUserId Long memberId,
                                      @Valid @RequestBody DiaryRequest diaryRequest) {
        DiaryIdResponse response = diaryService.saveDiary(diaryRequest, memberId);
        return Response.ofSuccess("OK", response);
    }

    @Operation(summary = "특정 일기 조회")
    @GetMapping("/{id}")
    public Response<Object> findDiary(@LoginUserId Long memberId, @PathVariable Long id) {
        DiaryResponse response = diaryService.findOne(memberId, id);
        return Response.ofSuccess("OK", response);
    }

    @Operation(summary = "특정 일기 수정")
    @PutMapping("/{id}")
    public Response<Object> updateDiary(@LoginUserId Long memberId, @PathVariable Long id,
                                        @RequestBody DiaryRequest diaryRequest) {
        DiaryResponse response = diaryService.updateDiary(diaryRequest, memberId, id);
        return Response.ofSuccess("OK", response);
    }

    @Operation(summary = "특정 일기 삭제")
    @DeleteMapping("/{id}")
    public Response<Object> deleteDiary(@LoginUserId Long memberId, @PathVariable Long id) {
        DiaryIdResponse response = diaryService.deleteDiary(memberId, id);
        return Response.ofSuccess("OK", response);
    }
}