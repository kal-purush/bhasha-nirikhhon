package com.example.demo.course.controller;

import com.example.demo.course.dto.CourseCommentDTO;
import com.example.demo.course.service.CourseCommentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.util.List;

@RestController
@RequestMapping("/courseComment")
public class CourseCommentController {

    @Autowired
    CourseCommentService service;

//    코스게시물별 댓글 목록 조회
    @GetMapping("/list")
    public List<CourseCommentDTO> list(@RequestParam(name="CourseNo") String courseNo){
        List<CourseCommentDTO> commentList = service.getListByComment(courseNo);
        return commentList;
    }

    @PostMapping("/register")
    public Boolean register(@RequestBody CourseCommentDTO dto,
                            Principal principal){
        String id = principal.getName();
        dto.setUserId(id);
        service.register(dto);

        return true;
    }

    @DeleteMapping("/remove")
    public Boolean remove(@RequestParam(name = "CourseCommentNo")int courseCommentNo){
        boolean result = service.remove(courseCommentNo);
        return result;
    }
}