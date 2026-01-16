package com.chwipoClova.interview.request;

import com.chwipoClova.qa.request.QaAnswerDataInsertReq;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.List;

@Data
public class InterviewQaAnswerInsertReq {

    @Schema(description = "유저 ID", example = "1", name = "userId")
    private Long userId;

    @Schema(description = "면접 ID", example = "1", name = "interviewId")
    private Long interviewId;

    @Schema(description = "답변데이터", name = "answerData")
    private List<QaAnswerDataInsertReq> answerData;
}