package com.imooc.passbook.customerplatform.service;

import com.imooc.passbook.customerplatform.vo.Feedback;

import java.util.List;

public interface IFeedbackService {

    void createFeedback(Feedback feedback);

    List<Feedback> getFeedbackByUserId(Long userId);
}