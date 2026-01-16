package com.example.tagletagle.src.board.dto;

import java.util.List;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "댓글 작성을 위한 DTO")
@AllArgsConstructor
@NoArgsConstructor
public class CreateCommentDTO {

    @Schema(description = "댓글 내용")
    private String content;

}