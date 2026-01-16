package com.example.moreveiw.domain.project.model.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PostProjectMemberRequest {
    @Schema(description = "방 ID", example = "hash 값")
    @NotEmpty
    @Size(max = 500)
    public String roomId;

    @Schema(description = "프로젝트 맴버", example = "프로젝트 맴버")
    @NotEmpty
    @Size(max = 5000)
    public Long memberId;
}