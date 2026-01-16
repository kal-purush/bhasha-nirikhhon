package org.detective.controller.UserPoint;

import lombok.RequiredArgsConstructor;
import org.detective.repository.ChatRoomRepository;
import org.detective.repository.UserPointRepository;
import org.detective.services.UserPointService;
import org.detective.services.chat.ChatRoomService;
import org.detective.util.CustomUserDetails;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class UserPointController {
    private final UserPointService userPointService;
    private final ChatRoomService chatRoomService;

    @PostMapping("/chatroom/{chatRoomId}/unlimitedChat")
    public ResponseEntity<String> usePoints(@AuthenticationPrincipal CustomUserDetails user, @RequestParam Long points, @PathVariable String chatRoomId) {
        Long userId = user.getUserId();
        try {
            userPointService.usePoints(userId, points, chatRoomId);
            return ResponseEntity.ok(points + "사용");
        } catch (RuntimeException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }
}