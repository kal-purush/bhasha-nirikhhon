package com.singo.Plan;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/planController")
@CrossOrigin
public class PlanController {

    @Autowired
    private PlanService planService;

    // add Plan
    @PostMapping("/addPlan")
    public void addPlan( @RequestParam("planType") String planType,@RequestParam("planEventId")String planEventId,@RequestParam("planDayId")String planDayId ){
           planService.addPlan(planType,Integer.parseInt(planEventId),Integer.parseInt(planDayId));
    }
}