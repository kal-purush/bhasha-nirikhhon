package com.teachsync.controllers;

import com.teachsync.dtos.session.SessionCreateDTO;
import com.teachsync.dtos.session.SessionReadDTO;
import com.teachsync.dtos.session.SessionUpdateDTO;
import com.teachsync.dtos.user.UserReadDTO;
import com.teachsync.services.session.SessionService;
import com.teachsync.utils.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@Controller
public class SessionController {
    @Autowired
    private SessionService sessionService;


    /* =================================================== CREATE =================================================== */
    @PostMapping("/create-session")
    public String createSession(
            @RequestParam("id") Long clazzId,
            @ModelAttribute List<SessionCreateDTO> sessionCreateDTOList,
            @SessionAttribute(value = "user", required = false) UserReadDTO userDTO
    ) {
        if (userDTO == null) {
            return "redirect:/index";
        }

        if (!userDTO.getRoleId().equals(Constants.ROLE_ADMIN)) {
            return "redirect:/index";
        }

        try {
            List<SessionReadDTO> sessionReadDTOList =
                    sessionService.createBulkSessionByDTO(sessionCreateDTOList);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "redirect:/edit-schedule"+"?id=" + clazzId;
    }


    /* =================================================== READ ===================================================== */
    @GetMapping("/session")
    public String sessionPage(
            @RequestParam Long clazzId,
            @RequestParam LocalDate from,
            @RequestParam LocalDate to,
            @SessionAttribute(value = "user", required = false) UserReadDTO userDTO
    ) {
        if (userDTO == null) {
            return "redirect:/index";
        }

        if (!userDTO.getRoleId().equals(Constants.ROLE_ADMIN)) {
            return "redirect:/index";
        }


        return "";
    }


    /* =================================================== UPDATE =================================================== */
//    @GetMapping("/edit-session")
//    public String editSessionPage(
//            @RequestParam("id") Long clazzId,
//            @ModelAttribute List<SessionUpdateDTO> sessionCreateDTOList,
//            @SessionAttribute(value = "user", required = false) UserReadDTO userDTO
//    ) {
//        if (userDTO == null) {
//            return "redirect:/index";
//        }
//
//        if (!userDTO.getRoleId().equals(Constants.ROLE_ADMIN)) {
//            return "redirect:/index";
//        }
//
//        try {
//            List<SessionReadDTO> sessionReadDTOList =
//                    sessionService.createBulkSessionByDTO(sessionCreateDTOList);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return "redirect:/edit-schedule"+"?id=" + clazzId;
//    }


    /* =================================================== DELETE =================================================== */

}