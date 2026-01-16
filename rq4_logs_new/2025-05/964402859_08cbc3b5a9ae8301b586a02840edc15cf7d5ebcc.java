package com.fatepet.petrest.business.admin.dto;

import lombok.Builder;
import lombok.Getter;
import org.springframework.web.multipart.MultipartFile;

@Getter
@Builder
public class BusinessInfoDto {
    private String name;
    private String category;
    private String address;
    private Double latitude;
    private Double longitude;
    private String businessHours;
    private String phoneNumber;
    private String email;
    private String additionalInfo;
    private MultipartFile thumbnail;
}
