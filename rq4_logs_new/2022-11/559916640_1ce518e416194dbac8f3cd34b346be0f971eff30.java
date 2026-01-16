package com.project.pds.user.controller;

import com.project.pds.user.service.PdsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.Enumeration;
import java.util.HashMap;

@Controller
public class UserFileController {
    @Autowired
    private PdsService pdsService;

    @RequestMapping("/pds/profileUpdateForm")
    public String profileUpdateForm(){
        return "users/profileUpdate";
    }

    @RequestMapping("/profileUpdate")
    public String profileUpdate(@RequestParam HashMap<String, Object> map,
                                HttpServletRequest request,
                                HttpSession httpSession){
        System.out.println(map);
        pdsService.setWrite(map, request, httpSession);

        return "users/getUser";
    }
}