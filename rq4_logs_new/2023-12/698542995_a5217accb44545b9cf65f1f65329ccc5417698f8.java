package com.example.backend.controller;

import com.example.backend.controller.request.UserSubjectRequestDTO;
import com.example.backend.controller.response.Response;
import com.example.backend.controller.response.UserLoginResponseDTO;
import com.example.backend.controller.response.UserSubjectResponseDTO;
import com.example.backend.service.UserSubjectService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;
import java.util.List;

@RestController
@RequestMapping("/api/user-subjects")
@RequiredArgsConstructor
public class UserSubjectController {

    @Autowired
    private final UserSubjectService userSubjectService;

    @PostMapping
    public Response<UserSubjectResponseDTO> setSubjects(@RequestBody UserSubjectRequestDTO requestDTO, HttpSession session) {
        UserLoginResponseDTO loginUser = (UserLoginResponseDTO) session.getAttribute("loginUser");
        if (loginUser == null) {
            return null;
        }
        UserSubjectResponseDTO responseDTO = userSubjectService.setSubjects(loginUser, requestDTO);
        return Response.success(responseDTO);
    }

    @GetMapping
    public Response<List<String>> getSubjectNames(HttpSession session) {
        UserLoginResponseDTO loginUser = (UserLoginResponseDTO) session.getAttribute("loginUser");
        if (loginUser == null) {
            return null;
        }

        List<String> subjectNames = userSubjectService.getSubjectNames(loginUser);
        return Response.success(subjectNames);
    }
}