package com.codeup.mentor.services;

import com.codeup.mentor.model.Rating;
import com.codeup.mentor.repositories.RatingRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RatingService {
    @Autowired
    private RatingRepository ratingRepository;

    public Integer allRatingsOnSearch(long id){
        int avgRating = 0;

        List<Rating> ratingList = ratingRepository.findAllByRecipientId(id); //replace with id
        for(Rating rating : ratingList){
            avgRating += rating.getRating();
        }
        avgRating = avgRating / ratingList.size();

        return avgRating;
    }
}