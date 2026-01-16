package com.chapssal_tteok.preview.domain.resume.controller;

import com.chapssal_tteok.preview.domain.resume.converter.ResumeConverter;
import com.chapssal_tteok.preview.domain.resume.dto.ResumeRequestDTO;
import com.chapssal_tteok.preview.domain.resume.dto.ResumeResponseDTO;
import com.chapssal_tteok.preview.domain.resume.entity.Resume;
import com.chapssal_tteok.preview.domain.resume.service.ResumeCommandService;
import com.chapssal_tteok.preview.domain.resume.service.ResumeQueryService;
import com.chapssal_tteok.preview.global.apiPayload.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth/resumes")
@RequiredArgsConstructor
public class ResumeController {

    private final ResumeCommandService resumeCommandService;
    private final ResumeQueryService resumeQueryService;

    @Operation(summary = "자기소개서 생성", description = "새로운 자기소개서를 생성합니다.")
    @PostMapping
    public ApiResponse<ResumeResponseDTO.CreateResumeResultDTO> createResume(@RequestBody @Valid ResumeRequestDTO.CreateResumeDTO request) {

        Resume resume = resumeCommandService.createResume(request);

        return ApiResponse.onSuccess(ResumeConverter.toCreateResumeResultDTO(resume));
    }

    @Operation(summary = "자기소개서 조회", description = "자기소개서 ID를 통해 특정 자기소개서를 조회합니다.")
    @GetMapping("/{resume_id}")
    public ApiResponse<ResumeResponseDTO.ResumeDTO> getResume(@PathVariable Long resume_id) {

        Resume resume = resumeQueryService.getResumeById(resume_id);

        return ApiResponse.onSuccess(ResumeConverter.toResumeDTO(resume));
    }

    @Operation(summary = "자기소개서 수정", description = "자기소개서 ID를 통해 특정 자기소개서를 수정합니다.")
    @PatchMapping("/{resume_id}")
    public ApiResponse<ResumeResponseDTO.ResumeDTO> updateResume(@PathVariable Long resume_id,
                                                                 @RequestBody @Valid ResumeRequestDTO.UpdateResumeDTO request) {

        Resume resume = resumeCommandService.updateResume(resume_id, request);

        return ApiResponse.onSuccess(ResumeConverter.toResumeDTO(resume));
    }

    @Operation(summary = "자기소개서 삭제", description = "자기소개서 ID를 통해 특정 자기소개서를 삭제합니다.")
    @DeleteMapping("/{resume_id}")
    public ApiResponse<String> deleteResume(@PathVariable Long resume_id) {

        resumeCommandService.deleteResume(resume_id);

        return ApiResponse.onSuccess("Deleted Resume with resume ID: " + resume_id);
    }
}