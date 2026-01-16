package com.roomfit.be.survey.infrastructure.support;

import com.roomfit.be.survey.application.dto.OptionDTO;
import com.roomfit.be.survey.application.dto.ReplyDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ReplyRepositoryImpl implements  ReplyRepository{
    private final JdbcTemplate jdbcTemplate;
    @Override
    public void saveBulkReply(ReplyDTO.Create request) {
        String sql = "INSERT INTO replies (reply_label, reply_value, question_id) VALUES (?, ?, ?)";
        List<Object[]> optionParams = new ArrayList<>();
        for(ReplyDTO.QuestionReply questionReply :request.getQuestionReplies()){
            Long id = questionReply.getQuestionId();
            List<OptionDTO.Request> replies = questionReply.getReplies();
            if(id != null){
                replies.forEach(reply->{
                    optionParams.add(new Object[]{
                            reply.getLabel(),
                            reply.getValue(),
                            id
                    });
                });
            }
        }
        jdbcTemplate.batchUpdate(sql, optionParams);
    }
}