package by.bsuir.tattoo4u.dto.response;

import lombok.Data;

import java.util.List;

@Data
public class MasterCommentAfterAddingResponseDto {

    private Double masterNewRating;
    private List<MasterCommentResponseDto> comments;

    public MasterCommentAfterAddingResponseDto(Double masterNewRating, List<MasterCommentResponseDto> comments) {
        this.masterNewRating = masterNewRating;
        this.comments = comments;
    }
}