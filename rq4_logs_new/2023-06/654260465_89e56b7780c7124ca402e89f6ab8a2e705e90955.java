package com.nbp.ala_travel.controller;

import com.nbp.ala_travel.service.ReviewService;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@AllArgsConstructor
@RequestMapping("reviewController")
public class ReviewController {
    private final ReviewService reviewService;

    @GetMapping("/reviewForm")
    public String getReviewForm(Model model) {
        model.addAttribute("bodyContent", "review-form");
        return "master-template";
    }

    @PostMapping("/createReview")
    public String createReview(Model model,
                               @RequestParam Long personId,
                               @RequestParam Integer grade,
                               @RequestParam String description,
                               @RequestParam Long tourId) {
        String result = reviewService.createReview(personId, grade, description, tourId);
        if (result.equals("OK")) {
            model.addAttribute("insertIntoReviewAndCheck", result);
            model.addAttribute("personId", personId);
            model.addAttribute("grade", grade);
            model.addAttribute("description", description);
            model.addAttribute("tourId", tourId);
            return "redirect:/home";
        } else {
            model.addAttribute("errorMsg", result);
            model.addAttribute("bodyContent","error");
            return "master-template";
        }
    }
}