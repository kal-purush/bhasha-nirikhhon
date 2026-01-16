package com.golfzon.social.album.controller;

import com.golfzon.social.album.service.AlbumService;
import com.golfzon.social.meeting.dto.MeetingDto;
import com.golfzon.social.meeting.service.MeetingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/meeting")
public class AlbumController {

    private final AlbumService albumService;

    //앨범등록하기
    @PostMapping(value = "/{meetingId}/post-album")
    public ResponseEntity<String> postAlbum(@PathVariable(name = "meetingId") Long meetingId, @RequestPart(value = "imageUrls") List<MultipartFile> files) {
//        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
//        log.info("principal:{}",principal);
        //Member member = ((UserDetailsImpl)principal).getMember();

        return ResponseEntity.ok()
                .contentType(new MediaType("application", "json", StandardCharsets.UTF_8))
                .body(albumService.postAlbum(meetingId,files)); //member
    }

    //앨범 사진조회
    @GetMapping(value = "/{meetingId}/albums")
    public ResponseEntity<List<String>> getMeeting(@PathVariable(name = "meetingId") Long meetingId) {
//        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
//        log.info("principal:{}",principal);
        //Member member = ((UserDetailsImpl)principal).getMember();

        // 업체 신청 목록보기
        return ResponseEntity.ok()
                .body(albumService.getAlbums(meetingId));
    }
}