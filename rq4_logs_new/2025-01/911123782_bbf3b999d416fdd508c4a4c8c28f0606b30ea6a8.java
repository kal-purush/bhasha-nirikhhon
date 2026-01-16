package com.roomfit.be.survey.presentation;

import com.roomfit.be.survey.application.SurveyService;
import com.roomfit.be.survey.application.dto.QuestionnaireDTO;
import com.roomfit.be.survey.application.dto.SurveyReplyDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController()
@RequestMapping("/api/v1/survey")
@RequiredArgsConstructor
public class SurveyController {
    private final SurveyService surveyService;

    @PostMapping("")
    QuestionnaireDTO.Response createQuestionnaire(@RequestBody QuestionnaireDTO.Create request){
        return surveyService.createQuestionnaire(request);
    }
    @GetMapping("")
    QuestionnaireDTO.Response readQuestionnaire(){
        return surveyService.readLatestQuestionnaire();
    }
    @PostMapping("/reply")
    SurveyReplyDTO.QuestionReply createReply(@RequestBody SurveyReplyDTO.CreateReply request){
        return surveyService.createReply(request);
    }
    @GetMapping("/reply")
    SurveyReplyDTO.QuestionReply readReply(@RequestParam("user_id") Long id){
        return surveyService.readReplyByUserId(id);
    }
}