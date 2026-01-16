package com.muban.controller;
import com.alibaba.fastjson.JSON;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;


@Controller
public class Tastajax {

    @RequestMapping("test")
    public String main2(){
        return "test";
    }

    @RequestMapping("test1")
    @ResponseBody
    public String testajax(@RequestBody String a){
        Map<String, Object> map=new HashMap<String, Object>();
        map.put("fd","好受点");
        return "success";

    }


}