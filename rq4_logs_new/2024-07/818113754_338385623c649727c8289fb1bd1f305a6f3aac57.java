package com.likelion.hackathon.controller;

import org.springframework.web.bind.annotation.RestController;

import com.likelion.hackathon.domain.User;
import com.likelion.hackathon.dto.request.friend.CreateFriendRequest;
import com.likelion.hackathon.dto.request.friend.CreateGuestbookRequest;
import com.likelion.hackathon.dto.response.friend.FriendSearchElement;
import com.likelion.hackathon.service.FriendService;
import com.likelion.hackathon.service.UserService;

import lombok.RequiredArgsConstructor;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;



@RestController
@RequiredArgsConstructor
public class FriendController {
    private final UserService userService;
    private final FriendService friendService;

    @GetMapping("/friend")
    public ResponseEntity getFriendList() {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        User user = (User) userService.loadUserByUsername(userId);
        if (user == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("사용자 정보가 올바르지 않습니다.");
        }
        return ResponseEntity.ok(friendService.getFriendList(userId));
    }

    @PostMapping("/friend/create")
    public ResponseEntity createFriend(@RequestBody CreateFriendRequest dto) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        User user = (User) userService.loadUserByUsername(userId);
        if (user == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("사용자 정보가 올바르지 않습니다.");
        }
        friendService.createFriend(userId, dto);
        return ResponseEntity.status(HttpStatus.CREATED).body("친구 추가가 완료되었습니다.");
    }
    
    @GetMapping(value = "/friend/search", params = "id")
    public ResponseEntity searchFriend(@RequestParam String id) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        User user = (User) userService.loadUserByUsername(userId);
        if (user == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("사용자 정보가 올바르지 않습니다.");
        }
        List<FriendSearchElement> result = friendService.friendSearch(id, userId);
        if (result.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("검색 결과가 없습니다.");
        }
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/friend/icecream/{id}")
    public ResponseEntity getFriendIcecream(@PathVariable String id) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        User user = (User) userService.loadUserByUsername(userId);
        if (user == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("사용자 정보가 올바르지 않습니다.");
        }
        return ResponseEntity.ok(friendService.getFriendIcecream(id));
    }

    @PostMapping("/friend/guestbook")
    public ResponseEntity createGuestbook(@RequestBody CreateGuestbookRequest dto) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        User user = (User) userService.loadUserByUsername(userId);
        if (user == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("사용자 정보가 올바르지 않습니다.");
        }
        friendService.createGuestbook(user.getName(), dto);
        return ResponseEntity.status(HttpStatus.CREATED).body("방명록 작성이 완료되었습니다.");
    }
}