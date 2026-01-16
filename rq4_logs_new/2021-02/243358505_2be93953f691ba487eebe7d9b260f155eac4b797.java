package mybase.controllers;

import lombok.AllArgsConstructor;
import mybase.domain.dto.GoalDto;
import mybase.domain.dto.NewGoalDto;
import mybase.domain.jpa.MainUser;
import mybase.services.GoalsService;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/goals")
@AllArgsConstructor
public class GoalsController {

    private final GoalsService goalsService;





    @PostMapping("/add/new")
    public GoalDto addNewGoal(@RequestBody NewGoalDto newGoalDto, @AuthenticationPrincipal MainUser mainUser) {



        return goalsService.addNewGoal(newGoalDto, mainUser);//new GoalDto();
    }


}