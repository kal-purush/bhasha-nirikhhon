package com.example.demo.controller;

import com.example.demo.dto.VideoCommentDTO;
import com.example.demo.entity.VideoCommentEntity;
import com.example.demo.service.VideoCommentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/VideoComments")
public class VideoCommentController {

    @Autowired
    private VideoCommentService commentsService;

    @GetMapping("/numberOfComments/{videoId}")
    public ResponseEntity<Integer> numberOfComments(@PathVariable("videoId") int videoId) {
        int numberOfComments = commentsService.numberOfComments(videoId);
        if (numberOfComments >= 0) {
            return ResponseEntity.ok(numberOfComments);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/allComments/{videoId}")
    public ResponseEntity<List<VideoCommentDTO>> getComments(@PathVariable("videoId") int videoId) {
        List<VideoCommentDTO> commentDTOList = commentsService.getComments(videoId);
        if (commentDTOList.isEmpty()) {
            return ResponseEntity.notFound().build();
        } else {
            return ResponseEntity.ok(commentDTOList);
        }
    }

    @PostMapping("/saveComment")
    public ResponseEntity<String> saveComment(@RequestParam("videoId") int videoId,
                                              @RequestParam("commenterName") String commenterName,
                                              @RequestParam("comment") String comment) {
        try {
            VideoCommentEntity commentEntity = new VideoCommentEntity();
            commentEntity.setVideoId(videoId);
            commentEntity.setCommenterName(commenterName);
            commentEntity.setComment(comment);
            commentsService.saveComment(commentEntity);
            return ResponseEntity.ok().body("successful");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed comment");
        }

    }

    @DeleteMapping("delete/{id}")
    public ResponseEntity<Void> deletePost(@PathVariable("id") int id) {
        commentsService.deleteComment(id);
        return ResponseEntity.noContent().build();
    }
}