package com.office.system.modlues.rlM.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author Chen
 * @createTime 2019117 14:34
 * @description the controller of RlLeave
 */
@Controller
@RequestMapping("/RlLeave")
public class RlLeaveController {

    /**
     * @Description  //TODO to RlLeave.html
     * @Author Chen
     * @DateTime 2019/1/17
     * @Param
     * @Return
     */
    @RequestMapping(value = "/leavePage.do")
    public Object toLeavePage() {
        return "moudlues/rlM/rlLeave_add";
    }

    @RequestMapping(value = "/submitLeave.do")
    public Object submitLeave() {
        return null;
    }
}