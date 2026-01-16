package com.example.demo.Landmark.controller;

import com.example.demo.Landmark.dto.LandmarkCommentDTO;
import com.example.demo.Landmark.service.LandmarkCommentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.util.List;

@RestController
@RequestMapping("/landmarkComment")
public class LandmarkCommentController {

    @Autowired
    LandmarkCommentService service;

//    게시물별 댓글 목록 조회
    @GetMapping("/list")
    public List<LandmarkCommentDTO> list(@RequestParam(name = "landmarkNo") int landmarkNo){
        List<LandmarkCommentDTO> commentList = service.getListByLandmarkNo(landmarkNo);
//        ↓댓글목록반환   ↑게시물기준,댓글목록조회
        return commentList;
    }

    @PostMapping("/register")
    public Boolean register(@RequestBody LandmarkCommentDTO dto, Principal principal){
        String id = principal.getName();    //인증객체 아이디를 꺼내서, 작성자로 입력 **userID가 뭔줄알고 가져오는지?**
        dto.setWriter(id);
        service.register(dto);
//        참고) Principal은 사용자 인증과 권한 부여와 관련된 개념으로,주로 인증된 사용자를 나타내는 객체를 의미
        return true;
    }

    @DeleteMapping("/remove")
    public Boolean remove(@RequestParam(name = "landmarkcommentNo") int landmarkcommentNo) {
        boolean result = service.remove(landmarkcommentNo);

        return result;
    }
}