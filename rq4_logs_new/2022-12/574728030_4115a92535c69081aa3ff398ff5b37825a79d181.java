package com.golfzon.social.round.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.web.multipart.MultipartFile;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class RoundResponseDto {

    private String explanation; // 라운드 설명
    private String roundDate; // 라운드 진행 날짜
    private String recruitStart; // 라운드 모집 시작
    private String recruitEnd; // 라운드 모집 마감
    private String location; // 장소
    private String details; // 상세주소
    private String minAge; // 최소 연령 조건
    private String maxAge; // 최대 연령 조건
    private String personnel; // 인원 수
    private String gender; // 성별
    private String imageUrl; // 라운드 대표 이미지
}