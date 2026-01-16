package com.company.getyourgoal.controller;

import com.company.getyourgoal.model.Goal;
import com.company.getyourgoal.model.User;
import com.company.getyourgoal.repository.GoalRepository;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.awt.*;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class GoalControllerTest {

    @Autowired
    GoalRepository goalRepository;
    @Before
    public void setUp() throws Exception {
        goalRepository.deleteAll();

    }

    @Test
    public void addGetDeleteGoal() {
        Goal goal = new Goal();
        goal.setGoalTitle("Work out");
        goal.setGoal("At the end of year to lose 50lb");
        goal.setUserName("JDarren");
        goal.setUserId(23);
        goalRepository.save(goal);

        Optional<Goal> goal1 = goalRepository.findById(goal.getId());

        assertEquals(goal1.get(), goal);

        goalRepository.deleteById(goal.getId());

        goal1 = goalRepository.findById(goal.getId());

        assertFalse(goal1.isPresent());
    }

    @Test
    public void getAllGoals() {
        Goal goal = new Goal();
        goal.setGoalTitle("Work out");
        goal.setGoal("At the end of year to lose 50lb");
        goal.setUserName("JDarren");
        goalRepository.save(goal);

        goal = new Goal();
        goal.setGoalTitle("Job");
        goal.setGoal("Getting new job by 2022");
        goal.setUserName("Aaron");
        goalRepository.save(goal);

        List<Goal> goalList = goalRepository.findAll();
        assertEquals(goalList.size(),2);

    }

    @Test
    public void updateGoal() {

        Goal goal = new Goal();
        goal.setGoalTitle("Work out");
        goal.setGoal("At the end of year to lose 50lb");
        goal.setUserName("JDarren");
        goalRepository.save(goal);

        goal.setGoalTitle("New Title");
        goal.setGoal("New Goal");
        goal.setUserName("New Username");
        goalRepository.save(goal);


        Optional<Goal> goal1 = goalRepository.findById(goal.getId());

        assertEquals(goal1.get(), goal);

    }

    @Test
    public void GetOneById() {
        Goal goal = new Goal();
        goal.setGoalTitle("Work out");
        goal.setGoal("At the end of year to lose 50lb");
        goal.setUserName("JDarren");
        goalRepository.save(goal);

        Optional<Goal> goal1 = goalRepository.findById(goal.getId());
        Assert.assertEquals(goal1.get(), goal);
        goal1 = goalRepository.findById(goal.getId());
        assertTrue(goal1.isPresent());
    }
}