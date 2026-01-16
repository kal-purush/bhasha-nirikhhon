package com.inhatc.demp.controller;

import com.inhatc.demp.domain.Answer;
import com.inhatc.demp.dto.QuestionAnswer;
import com.inhatc.demp.repository.AnswerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/answer")
public class AnswerController {

    private final AnswerRepository answerRepository;

    @GetMapping("/{questionId}")
    public ResponseEntity<List<QuestionAnswer>> answers(@PathVariable Long questionId) {
        log.info("AnswerController.answers");
        List<Answer> answers = answerRepository.findByQuestion_Id(questionId);
        List<QuestionAnswer> result = answers.stream().map(a -> new QuestionAnswer(a)).collect(Collectors.toList());

        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}