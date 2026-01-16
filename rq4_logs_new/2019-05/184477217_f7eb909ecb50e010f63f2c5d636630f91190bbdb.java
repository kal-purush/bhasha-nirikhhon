package com.triviamachine.triviamachine.controllers;

import com.triviamachine.triviamachine.database.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;

import java.util.Date;
import java.util.List;

@Controller
@RequestMapping("/admin")
public class AdminPanelController {
    @Autowired
    private QuestionRepository questionRepo;
    @Autowired
    private QuestionScheduleRepository questionScheduleRepo;
    @Autowired
    private ResultsRepository resultsRepo;

    @GetMapping("/test")
    public RedirectView dummyData(){

        Question dummyQuestion = new Question();
        dummyQuestion.setQuestionText("Test!");
        dummyQuestion.setAnswerOne("ONE");
        dummyQuestion.setAnswerTwo("TWO");
        dummyQuestion.setAnswerThree("THREE");
        dummyQuestion.setAnswerFour("FOUR");
        questionRepo.save(dummyQuestion);

        QuestionSchedule testSched = new QuestionSchedule();
        testSched.setQuestion(dummyQuestion);
        testSched.setDate(new Date(1557903600));

        questionScheduleRepo.save(testSched);


        return new RedirectView("/admin");
    }

    @GetMapping("")
    public String adminPanelDefaultRoute(){



        return "redirect:/admin/schedule";
    }

    @GetMapping("/schedule")
    public String getSchedulePanel(Model model) {
        List<QuestionSchedule> schedules = questionScheduleRepo.findAll();
        model.addAttribute("schedules", schedules);
        return "admin/schedule";
    }

    @GetMapping("/question")
    public String getQuestionPanel(Model model){
        List<Question> questions = questionRepo.findAll();
        model.addAttribute("questions", questions);
        return "admin/question";
    }

    @GetMapping("/results")
    public String getResultsPanel(Model model){
        List<Results> results = resultsRepo.findAll();
        model.addAttribute("results", results);
        return "admin/results";
    }
}