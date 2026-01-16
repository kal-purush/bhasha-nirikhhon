package com.roomfit.be.user.presentation;

import com.roomfit.be.user.application.UserService;
import com.roomfit.be.user.application.event.CompleteSurveyEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class UserEventListener {
    private final UserService userService;
    @EventListener
    public void onReplyCreationSuccess(CompleteSurveyEvent completeSurveyEvent){
        userService.completeSurvey(completeSurveyEvent.getUserId());
    }


}