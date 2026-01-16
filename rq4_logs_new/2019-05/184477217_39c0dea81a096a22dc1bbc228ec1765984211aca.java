package com.triviamachine.triviamachine.controllers;

import com.google.gson.Gson;
import com.triviamachine.triviamachine.database.QuestionSchedule;
import com.triviamachine.triviamachine.database.QuestionScheduleRepository;
import com.triviamachine.triviamachine.database.Results;
import com.triviamachine.triviamachine.database.ResultsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

@RestController
@RequestMapping("/vote")
public class VoteController {
    @Autowired
    DateFormat dateFormat;
    @Autowired
    QuestionScheduleRepository questionScheduleRepo;
    @Autowired
    ResultsRepository resultsRepo;
    @Autowired
    Gson gson;


    @PostMapping("/{answer}")
    public String vote(@PathVariable int answer) throws ParseException {
        Date today = new Date(System.currentTimeMillis());
        String todayToString = dateFormat.format(today);
        today = dateFormat.parse(todayToString);

        QuestionSchedule currentQuestion = questionScheduleRepo.findByDate(today);
        Results currentResults = currentQuestion.getResults();

        currentResults.vote(answer);

        resultsRepo.save(currentResults);

        return gson.toJson(currentResults.convertToJson());
    }
}