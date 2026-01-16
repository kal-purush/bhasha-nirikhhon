package com.example.musing.board.dto;

import com.example.musing.artist.entity.Artist;
import com.example.musing.hashtag.entity.HashTag;
import com.example.musing.music.entity.Music;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class CreateBoardResponse {

    @Schema(description = "음악추천 고유 ID", example = "1")
    private Long id;

    @Schema(description = "사용자 이메일", example = "user@example.com")
    private String userEmail;

    @Schema(description = "게시글 제목", example = "지듣노")
    private String title;

    @Schema(description = "음악 제목", example = "노래 제목")
    private String musicTitle;

    @Schema(description = "아티스트", example = "가수 이름")
    private String artist;

    @Schema(description = "유튜브 링크", example = "https://youtube.com/video")
    private String youtubeLink;

    @Schema(description = "해시태그 목록", example = "[\"힙합\", \"추천\"]")
    private List<String> hashtags;

    @Schema(description = "장르", example = "힙합")
    private String genre;

    @Schema(description = "이미지 URL", example = "http://example.com/images/board/123.jpg")
    private String imageUrl;

    @Schema(description = "게시글 내용", example = "쌈뽕한 노래 소개")
    private String content;

    @Schema(description = "게시글 생성 시간", example = "2025-01-01T12:00:00")
    private LocalDateTime createdAt;

    @Schema(description = "게시글 수정 시간", example = "2025-01-01T14:00:00")
    private LocalDateTime updatedAt;



}