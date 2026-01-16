package com.roomfit.be.survey.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SurveyReplyDTO {

    private Long id;
    private List<QuestionReply> replies;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QuestionReply {
        private Long questionId;
        private List<ReplyDetail> reply;

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static class ReplyDetail {
            private String label;
            private String value;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateReply {
        private Long questionnaireId;
        private String[] answers;
    }
}