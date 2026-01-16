package org.example.canon.controller;

import lombok.RequiredArgsConstructor;
import org.example.canon.controller.request.ProfileRequest;
import org.example.canon.controller.response.postResponse.PostResponse;
import org.example.canon.dto.CustomOAuth2UserDTO;
import org.example.canon.dto.ProfileDTO;
import org.example.canon.service.ProfileService;
import org.example.canon.service.S3Uploader;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequiredArgsConstructor
@RequestMapping("/profile")
@CrossOrigin("*")
public class ProfileController {

    private final ProfileService profileService;
    private final S3Uploader s3Uploader;

    @PostMapping
    public ResponseEntity<PostResponse> uploadProfile(@RequestParam("image") MultipartFile image,
                                                      ProfileRequest profileRequest,
                                                      @AuthenticationPrincipal CustomOAuth2UserDTO userDto) throws IOException {
        String imageURL = s3Uploader.upload(image, "example");

        ProfileDTO profileDTO = ProfileDTO.of(profileRequest, imageURL);

        profileService.addProfile(profileDTO);
    }

}