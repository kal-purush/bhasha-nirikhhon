package com.example.musing.board.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UpdateBoardRequestDto {

    //업데이트 리퀘스트 dto
    private String userEmail;
    private String title;
    private String musicTitle;
    private String artist;
    private String youtubeLink;
    private List<String> hashtags;
    private String genre;
    private MultipartFile image;
    private String content;
}