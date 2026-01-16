package com.spring.boot.controller;

import com.spring.boot.dto.FeeDTO;
import com.spring.boot.dto.MemberDTO;
import com.spring.boot.service.EventService;
import com.spring.boot.service.FeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.List;

@Controller
public class FeeController {

    @Autowired
    private FeeService feeService;

    @RequestMapping(value = "/moneyManager")
    public String moneyManager(HttpServletRequest request) throws Exception{
        List<FeeDTO> fees = feeService.findAll();
        request.setAttribute("fees", fees);

        return "moneyManager";
    }

    @PostMapping(value = "/feeRegisterAction")
    public String feeRegisterAction(FeeDTO fee, HttpServletRequest request) throws Exception{
        feeService.save(fee);

        return moneyManager(request);
    }

    @RequestMapping(value = "/feeDeleteAction")
    public String feeDeleteAction(@RequestParam("id") int id, HttpServletRequest request) throws Exception{
        System.out.println(id);
        feeService.delete(id);

        return moneyManager(request);
    }

    @RequestMapping(value = "/feeHistory")
    public String feeHistory(HttpServletRequest request) throws Exception{
        List<FeeDTO> fees = feeService.findAll();
        request.setAttribute("fees", fees);

        return "moneyhistoryInquiry";
    }



}