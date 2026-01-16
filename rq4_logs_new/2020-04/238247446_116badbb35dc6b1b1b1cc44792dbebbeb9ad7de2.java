package by.bsuir.tattoo4u.dto.response;

import by.bsuir.tattoo4u.entity.MasterComment;
import by.bsuir.tattoo4u.entity.User;
import lombok.Data;

@Data
public class MasterCommentResponseDto {

    private String username;
    private String userPhoto;
    private Double rating;
    private String comment;

    public MasterCommentResponseDto(MasterComment masterComment){
        User user=masterComment.getAuthor();
        if(user!=null){
            this.username=user.getUsername();
            this.userPhoto=(user.getPhoto()!=null)?user.getPhoto().getUrl():null;
        }
        this.rating=masterComment.getRating();
        this.comment=masterComment.getComment();
    }
}