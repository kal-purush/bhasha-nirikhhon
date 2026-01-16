package com.example.EnjoyTripBackend.controller;

import com.example.EnjoyTripBackend.dto.place.PlaceRequestDto;
import com.example.EnjoyTripBackend.service.PlaceService;
import com.example.EnjoyTripBackend.service.S3Service;
import com.example.EnjoyTripBackend.util.SessionUser;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import static org.springframework.http.HttpStatus.CREATED;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class PlaceController {

    private final PlaceService placeService;
    private final S3Service s3Service;

    @PostMapping("/place")
    public ResponseEntity<String> save(PlaceRequestDto placeRequestDto, @SessionUser String userEmail,
                                       @RequestPart(value="file") MultipartFile file){
        String imageUrl = s3Service.upload(file);
        placeRequestDto.setPlaceImageUrl(imageUrl);
        placeService.save(userEmail, placeRequestDto);
        return ResponseEntity.status(CREATED).body("여행 장소 정보 등록 완료");
    }
}