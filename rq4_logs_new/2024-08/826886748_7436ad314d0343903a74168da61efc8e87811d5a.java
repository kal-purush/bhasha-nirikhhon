package com.umc.bwither.breeder.dto;

import com.umc.bwither.breeder.entity.enums.Animal;
import com.umc.bwither.breeder.entity.enums.EmploymentStatus;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class SignupDTO {
    // User 관련 필드
    private String name;           // 이름
    private String phone;          // 전화번호
    private String email;          // 이메일
    private String username;       // 아이디
    private String password;       // 비밀번호

    private String zipcode;        // 우편번호
    private String address;        // 주소
    private String addressDetail;  // 상세 주소

    // Breeder 관련 필드
    private Animal animal;         // 브리딩하는 동물 종류
    private String species;        // 종
    private String tradeName;      // 상호명
    private String tradePhone;     // 전화번호
    private String tradeEmail;     // 이메일
    private String representative; // 대표자명
    private String registrationNumber; // 사업자 등록 번호
    private String licenseNumber;  // 동물생산업 허가 번호
    private String snsAddress;     // 홈페이지/SNS 주소 (선택 사항)
    private String animalHospital; // 이용 중인 동물병원 (선택 사항)
    private EmploymentStatus employmentStatus; // 재직 상태

    // 켄넬/케터리 사진
    private List<String> kennelImages; // 켄넬/케터리 사진 URL 리스트

    // 자격증 정보 (선택 사항)
    private List<String> certificateNames; // 자격증 이름 리스트 (선택 사항)
    private List<String> certificateFiles; // 자격증 파일 URL 리스트 (선택 사항)

    // 번식 경력 정보
    private String breedingTradeName;      // 상호명
    private String breedingJoinDate;       // 입사 연월 (예: YYYY-MM)
    private String breedingLeaveDate;      // 퇴사 연월 (예: YYYY-MM)
    private Boolean breedingCurrentlyEmployed; // 재직 중 여부
}