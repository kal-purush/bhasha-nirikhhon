package reiner.example.movie.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reiner.example.movie.model.Review;
import reiner.example.movie.service.ReviewService;

import java.util.Map;

/**
 * @ClassName ReviewController
 * @Description TODO
 * @Author Matthiola
 * @Date 2025/3/11 15:29
 */
@RestController
@RequestMapping("/api/v1/reviews")
public class ReviewController {

    @Autowired
    private ReviewService reviewService;

    @PostMapping
    public ResponseEntity<Review> createReview(@RequestBody Map<String, String> payload) {
        return new ResponseEntity<Review>(reviewService.createReview(payload.get("body"), payload.get("imdbId")), HttpStatus.CREATED);

    }
}