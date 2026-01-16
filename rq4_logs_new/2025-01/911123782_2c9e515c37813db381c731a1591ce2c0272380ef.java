package com.roomfit.be.survey.application;


import com.roomfit.be.survey.application.dto.QuestionnaireDTO;
import com.roomfit.be.survey.application.dto.SurveyReplyDTO;

public interface SurveyService {
    QuestionnaireDTO.Response createQuestionnaire(QuestionnaireDTO.Create request);

    QuestionnaireDTO.Response createQuestionnaireLegacy(QuestionnaireDTO.Create request);

    SurveyReplyDTO.QuestionReply createReply(SurveyReplyDTO.CreateReply request);

    QuestionnaireDTO.Response readLatestQuestionnaire();

    SurveyReplyDTO.QuestionReply readReplyByUserId(Long id);
}