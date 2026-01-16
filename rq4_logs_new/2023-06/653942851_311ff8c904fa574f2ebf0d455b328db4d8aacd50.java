package com.example.mealme.controller.admin;

import com.example.mealme.dto.UserDto;
import com.example.mealme.service.admin.AdminService;
import com.example.mealme.vo.SearchVo;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/admins/v1/*")
public class AdminRestController {
    private final AdminService adminService;

    @GetMapping("/user")
    public List<UserDto> showUserList(){
        return adminService.findAll();
    }

    @GetMapping("/searchUserList")
    public List<UserDto> searchUserList(SearchVo searchVo){
        return adminService.searchUserList(searchVo);
    }
}