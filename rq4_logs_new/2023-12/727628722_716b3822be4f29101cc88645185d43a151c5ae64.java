package com.spartanullnull.otil.domain.user.controller;

import com.spartanullnull.otil.domain.user.dto.*;
import com.spartanullnull.otil.domain.user.service.*;
import com.spartanullnull.otil.security.Impl.*;
import jakarta.validation.*;
import lombok.*;
import lombok.extern.slf4j.*;
import org.springframework.http.*;
import org.springframework.security.core.annotation.*;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/api/users/profile")
@RequiredArgsConstructor
@Slf4j
public class UserProfileController {

    private final UserProfileService userProfileService;

    @GetMapping
    public ResponseEntity<UserProfileGetResponseDto> getUserProfile(
        @AuthenticationPrincipal UserDetailsImpl userDetails
    ) {
        return userProfileService.getUserProfile(userDetails.getUser().getId());
    }

    @PutMapping
    public ResponseEntity<UserProfileModifyResponseDto> modifyUserProfile(
        @Valid @RequestBody UserProfileModifyRequestDto userProfileModifyRequestDto,
        @AuthenticationPrincipal UserDetailsImpl userDetails
    ) {
        return userProfileService.modifyUserProfile(userDetails.getUser().getId(),
            userProfileModifyRequestDto);
    }
}