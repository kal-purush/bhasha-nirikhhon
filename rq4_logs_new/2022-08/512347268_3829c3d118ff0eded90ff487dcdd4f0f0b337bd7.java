package com.cakestation.backend.store.dto.response;

import com.cakestation.backend.store.domain.Store;
import com.cakestation.backend.store.dto.request.CreateStoreDto;
import lombok.*;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
@Getter
public class ShowStoreDto {
    private String name;

    private String address;

    private String businessHours;

    private String phone;

    private String photoUrl;

    private String webpageUrl;

    private String kakaoMapUrl;

    private Double score;

    public static Store toEntity(ShowStoreDto showStoreDto){
        return Store.builder()
                .name(showStoreDto.getName())
                .address(showStoreDto.getAddress())
                .businessHours(showStoreDto.getBusinessHours())
                .phone(showStoreDto.getPhone())
                .photoUrl(showStoreDto.getPhotoUrl())
                .webpageUrl(showStoreDto.getWebpageUrl())
                .kakaoMapUrl(showStoreDto.getKakaoMapUrl())
                .build();
    }
}