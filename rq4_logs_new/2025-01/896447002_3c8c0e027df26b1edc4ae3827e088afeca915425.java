package com.example.musing.board.dto;

import com.example.musing.board.entity.Board;
import java.time.LocalDateTime;
import java.util.List;
import lombok.Builder;

//nested record class 사용

public class BoardRequestDto {

    @Builder
    public record RecommendBoardFirstDto(
        String title,
        String content,
        String musicName,
        String artistName,
        int rating,
        String thumbNailLink
    ) {
        public static RecommendBoardFirstDto toDto(Board board) {
            return RecommendBoardFirstDto.builder()
                .title(board.getTitle())
                .content(board.getContent())
                .musicName(board.getMusic().getName())
                .artistName(board.getMusic().getArtist().getName())
                .rating(board.getRating())
                .thumbNailLink(board.getMusic().getThumbNailLink())
                .build();
        }
    }

    @Builder
    public record RecommendBoardDto(
        String title,
        String musicName,
        String artistName,
        String thumbNailLink,
        LocalDateTime createAt
    ) {
        public static RecommendBoardDto toDto(Board board) {
            return RecommendBoardDto.builder()
                .title(board.getTitle())
                .musicName(board.getMusic().getName())
                .artistName(board.getMusic().getArtist().getName())
                .thumbNailLink(board.getMusic().getThumbNailLink())
                .createAt(board.getCreatedAt())
                .build();
        }
    }

    @Builder
    public record BoardDto(
        String title,
        String musicName,
        String artistName,
        int rating,
        int replyCount,
        String thumbNailLink,
        List<String> genreList,
        List<String> moodList
    ) {
        public static BoardDto toDto(Board board , List<String> genreList, List<String> moodList) {
            return BoardDto.builder()
                .title(board.getTitle())
                .musicName(board.getMusic().getName())
                .artistName(board.getMusic().getArtist().getName())
                .rating(board.getRating())
                .replyCount(board.getReplyCount())
                .thumbNailLink(board.getMusic().getThumbNailLink())
                .genreList(genreList)
                .moodList(moodList)
                .build();
        }
    }
}