package com.infobip.interviewdemo.api;

import com.infobip.interviewdemo.dto.CodeReviewCreateRequest;
import com.infobip.interviewdemo.dto.CodeReviewDetailResponse;
import com.infobip.interviewdemo.service.CodeReviewService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("review")
@RequiredArgsConstructor
public class CodeReviewController {

    private final CodeReviewService codeReviewService;

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public Long createReview(@RequestBody CodeReviewCreateRequest codeReviewRequest){
        return codeReviewService.createCodeReview(codeReviewRequest);
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public CodeReviewDetailResponse getCodeReview(@RequestParam Long reviewId){
        return codeReviewService.getCodeReview(reviewId);
    }

}