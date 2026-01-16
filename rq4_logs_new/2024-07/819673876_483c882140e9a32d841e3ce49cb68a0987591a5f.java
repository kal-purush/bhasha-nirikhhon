package com.example.demo.member.controller;


import com.example.demo.Util.FileUtil;
import com.example.demo.member.dto.MemberDTO;
import com.example.demo.member.dto.MemberTagDTO;
import com.example.demo.member.entity.Member;
import com.example.demo.member.entity.MemberTag;
import com.example.demo.member.service.MemberService;
import com.example.demo.member.service.MemberTagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/membertag")
public class MemberTagController {

    @Autowired
    private MemberTagService service;

    @Autowired
    MemberService memberService;

    @Autowired
    FileUtil fileUtil;

    @ResponseBody
    @GetMapping("/register")
    public ResponseEntity<Void> register(@RequestBody MemberTagDTO dto){
        service.register(dto);

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/get")
    public ResponseEntity<List> registerPost(@RequestParam(name = "id")String userId){

        List<MemberTagDTO> list = service.getList(userId);
        return new ResponseEntity<>(list, HttpStatus.OK);
    }

    @GetMapping("/remove")
    public ResponseEntity<Void> remove(@RequestParam(name = "id")String userId){
        service.removeAll(userId);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}