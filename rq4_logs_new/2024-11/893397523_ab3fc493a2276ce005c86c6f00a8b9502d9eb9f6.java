package com.aos.controller;

import com.aos.pojo.BreakdownInfo;
import com.aos.service.BreakdownInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * @Author：bingfeng
 * @Date：2024/11/24 15:59
 */
@Controller
@RequestMapping("/breakDownInfo")
public class BreakdownInfoController {
    @Autowired
    private BreakdownInfoService breakdownInfoService;

    @RequestMapping("/getAllBreakDownInfo")
    public String getAllBreakDownInfo(Model model) {
        List<BreakdownInfo> list = breakdownInfoService.getAllBreakdownInfo();
        model.addAttribute("list", list);
        return "breakDownInfo/list";
    }

    @RequestMapping("/toAddBreakDownInfo")
    public String toAddBreakDownInfo() {
        return "breakDownInfo/add";
    }

    @RequestMapping("/queryBreakDownInfoByColumn")
    public String queryBreakDownInfoByColumn(BreakdownInfo breakdownInfo) {
        return "redirect:/breakDownInfo/getAllBreakDownInfo";
    }



}