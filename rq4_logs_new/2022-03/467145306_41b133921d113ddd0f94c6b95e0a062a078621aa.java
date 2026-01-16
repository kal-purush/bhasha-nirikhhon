package com.company.randomgoalservice.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RestController
public class RandomGoalController {

    private List<String> goalList = new ArrayList<>();

    private Random randomGoal = new Random();

    public RandomGoalController() {
        goalList.add("Become an inspiration to others.\n");
        goalList.add("Master a difficult skill.\n");
        goalList.add("Become a thought leader in your industry.\n");
        goalList.add("Learn about how to become a millionaire.\n");
        goalList.add("Go on a trip around the world.\n");
        goalList.add("Travel to your dream country.\n");
        goalList.add("Double your personal income.\n");
        goalList.add("Get promoted to an executive role at your company.\n");
        goalList.add("Donate to a charity annually.\n");
        goalList.add("Worry Less, Smile More!\n");
    }

    @GetMapping("/randomGoals")
    @ResponseStatus(HttpStatus.OK)
    public String getRandomGoal() {
        int whichGoal = randomGoal.nextInt(9);
        return goalList.get(whichGoal);
    }
}