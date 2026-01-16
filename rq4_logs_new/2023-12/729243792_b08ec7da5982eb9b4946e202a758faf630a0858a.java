package com.chwipoClova.interview.response;

import com.chwipoClova.qa.response.QaListRes;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
@Builder
public class InterviewNotCompRes {
    @Schema(description = "면접 ID", example = "1", name = "interviewId")
    private Long interviewId;

    @Schema(description = "유저 ID", example = "1", name = "userId")
    private Long userId;

    @Schema(description = "총개수", example = "10", name = "totalCnt")
    private Integer totalCnt;

    @Schema(description = "답변개수", example = "1", name = "useCnt")
    private Integer useCnt;

    @Schema(description = "마지막 질문 ID", example = "1", name = "lastQaId")
    private Long lastQaId;

    @Schema(description = "질문데이터", name = "qaData")
    private List<QaListRes> qaData;
}