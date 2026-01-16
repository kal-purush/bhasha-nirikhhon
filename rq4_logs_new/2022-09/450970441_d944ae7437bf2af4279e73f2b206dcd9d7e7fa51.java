package whoami.core.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import whoami.core.dto.guestbookComment.GuestbookCommentResponseDto;
import whoami.core.dto.guestbookComment.GuestbookCommentSaveRequestDto;
import whoami.core.service.GuestbookCommentService;

@RestController
@RequestMapping("/guestbook/comment")
public class GuestbookCommentController {
    private GuestbookCommentService guestbookCommentService;

    @Autowired
    public GuestbookCommentController(GuestbookCommentService guestbookCommentService) {
        this.guestbookCommentService = guestbookCommentService;
    }

    @GetMapping(value = "/{guestbookId}")
    public ResponseEntity getGuestbook(@PathVariable Long guestbookCommentId) {
        GuestbookCommentResponseDto guestbookCommentResponseDto = guestbookCommentService.findByGuestbookCommentId(guestbookCommentId);
        return ResponseEntity.ok().body(guestbookCommentResponseDto);
    }

    @PostMapping(value = "/")
    public ResponseEntity createGuestbookComment(@RequestBody GuestbookCommentSaveRequestDto guestbookCommentSaveRequestDto) {
        GuestbookCommentResponseDto guestbookCommentResponseDto
                = guestbookCommentService.createGuestbookComment(guestbookCommentSaveRequestDto);
        return ResponseEntity.ok().body(guestbookCommentResponseDto);
    }

    @DeleteMapping(value = "/{guestbookCommentId}")
    public ResponseEntity<?> deleteGuestbookComment(@PathVariable Long guestbookCommnetId) {
        guestbookCommentService.deleteByGuestbookCommentId(guestbookCommnetId);
        return ResponseEntity.ok().body("Success");
    }
}