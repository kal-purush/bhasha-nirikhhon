package com.example.controller;

import com.example.Configuration.CountConfig;
import com.example.controllerHelper.CountControllerHelper;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

@Controller
public class DsspController
{
    @RequestMapping(value = "/chemistry/dssp", method = RequestMethod.GET)
    public String getCount(ModelMap model)
    {
        return "dssp";
    }

    @RequestMapping(value = "/chemistry/dssp", method = RequestMethod.POST)
    public String getResult(@RequestParam String text, Map<String, Object> model)
    {
        return "dssp";
    }
}