package cn.techyang.springboot.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author tecyang@gmail.com
 * @version V1.0
 * @title
 * @description
 * @date 2023-06-04 09:59
 */
@RestController
public class HelloWordController {

    @RequestMapping(value = "/hello", method = RequestMethod.GET)
    public String hello() {
        return "hello world!";
    }
}