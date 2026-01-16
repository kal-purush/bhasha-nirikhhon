package com.hangout.hangout.domain.image.controller;

import com.hangout.hangout.domain.image.service.UserImageFileUploadService;
import com.hangout.hangout.global.error.ResponseEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

import static com.hangout.hangout.global.common.domain.entity.Constants.API_PREFIX;
import static com.hangout.hangout.global.error.ResponseEntity.successResponse;

@RestController
@RequiredArgsConstructor
@RequestMapping(API_PREFIX + "/user")
@Slf4j
public class UserImageController {
    private final UserImageFileUploadService userImageFileUploadService;

    @PostMapping("/{userId}/image")
    public ResponseEntity<HttpStatus> uploadImages(@PathVariable Long userId,
                                                   @RequestParam("file") List<MultipartFile> files) throws IOException {
        userImageFileUploadService.upload(userId,files);

        return successResponse();
    }
}