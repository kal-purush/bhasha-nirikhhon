package com.hahachiu.Controller;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class ArticleController {

    @RequestMapping(value = "/getAll",method = RequestMethod.GET)
    @ResponseBody
    public String getUserAll() {

        return "test jenkin~";
    }
}