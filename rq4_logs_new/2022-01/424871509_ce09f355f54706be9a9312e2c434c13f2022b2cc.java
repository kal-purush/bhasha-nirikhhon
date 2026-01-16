package com.atable.alcholknowledge.controller;


import com.atable.alcholknowledge.model.WineInfo;
import com.atable.alcholknowledge.service.WineInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

@Controller
public class AdminController {
    private final WineInfoService wineInfoService;

    @Autowired
    public AdminController(WineInfoService wineInfoService){
        this.wineInfoService = wineInfoService;
    }


    @GetMapping("/admin/wineinfo")
    public String adminWineInfo(Model model){
        List<WineInfo> wineInfos = wineInfoService.findWineInfos();
        model.addAttribute("wineInfos", wineInfos);
        System.out.println("size of wineinfos : " + wineInfos.size());
        return "wineinfo/AdminWineInfoList";
    }


    @GetMapping("/admin")
    public String adminHome(Model model){
        return "adminHome";

    }
}