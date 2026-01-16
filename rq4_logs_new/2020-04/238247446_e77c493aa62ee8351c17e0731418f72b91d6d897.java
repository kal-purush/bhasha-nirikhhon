package by.bsuir.tattoo4u.dto.response;

import by.bsuir.tattoo4u.entity.StudioFeedback;
import lombok.Data;

@Data
public class StudioFeedbackResponseDto {
    private String username;
    //private String userPhoto;
    private String feedback;
    private String rating;

    public StudioFeedbackResponseDto(StudioFeedback studioFeedback) {
        this.feedback = studioFeedback.getFeedback();
        this.rating = studioFeedback.getRating().toString();
        this.username = studioFeedback.getUsername();
        //this.userPhoto = studioFeedback.getUserPhoto();
    }
}