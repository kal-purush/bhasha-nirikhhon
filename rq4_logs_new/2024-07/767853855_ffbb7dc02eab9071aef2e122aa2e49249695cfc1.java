package com.example.firstproject.controller;

import com.example.firstproject.dto.CommentDto;
import com.example.firstproject.entity.Article;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.ui.Model;

import java.util.List;

@Controller
@Slf4j
public class GroupController {
    @Autowired
    private GroupService groupService;
    @Autowired
    private TeamService teamService;

    @GetMapping("/groups/{id}") // 데이터 조회 요청 접수 - PathVariable
    public String show(@PathVariable Long groupId, Model model) { // 매개 변수로 id 받아오기
        // 1. id를 조회해 데이터 가져오기
        GroupDto groupDto = groupService.getGroup(groupId);
        List<TeamDto> teamDtos = teamService.getTeamsOnGroup(groupId);

        // 2. 모델에 데이터 등록하기
        model.addAttribute("groupname", groupDto.getName());
        model.addAttribute("teamDtoList", teamDtos);

        // 3. 조회한 데이터를 사용자에게 보여 주기 위한 뷰 페이지 만들고 반환하기
        return "groups/show";
    }

}