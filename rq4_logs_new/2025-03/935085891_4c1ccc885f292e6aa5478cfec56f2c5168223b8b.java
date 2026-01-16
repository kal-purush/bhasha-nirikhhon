package reiner.example.movie.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reiner.example.movie.model.Movie;
import reiner.example.movie.model.Review;
import reiner.example.movie.repository.ReviewsRepository;


/**
 * @ClassName ReviewService
 * @Description TODO
 * @Author Matthiola
 * @Date 2025/3/11 15:21
 */

@Service
public class ReviewService {
    @Autowired
    private ReviewsRepository reviewsRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    public Review createReview(String body, String imdbId) {
        Review review = new Review(body);
        reviewsRepository.insert(review);

        mongoTemplate.update(Movie.class)
                .matching(Criteria.where("imdbId").is(imdbId))
                .apply(new Update().push("reviewIds", review))
                .first();
        return review;
    }
}