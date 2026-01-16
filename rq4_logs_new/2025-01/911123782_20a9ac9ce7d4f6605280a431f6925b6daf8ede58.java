package com.roomfit.be.survey.application.dto;

import com.roomfit.be.survey.domain.Reply;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 맨 상위의 id 가 필요없음 jdbcTemplate 으로 바로 이를 넣기 떄문에.
 */
public class ReplyDTO {
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Create{
        private Long id;
        private List<QuestionReply> questionReplies;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QuestionReply{
        private Long questionId;
        private List<OptionDTO.Request> replies;
        public List<Reply> toRepliesEntity(){
            return replies.stream()
                    .map(OptionDTO.Request::toReplyEntity)
                    .toList();
        }
    }
}