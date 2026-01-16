package com.example.demo;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author orange
 * @descript
 * @Time 2017/5/31 0031
 */
//@RestController    //默认类中的方法都会以json的格式返回
@Controller
//@RequestMapping("v1")
public class Control {

    @RequestMapping(value = "love", method = RequestMethod.GET)
    @ResponseBody
    public String getOrange() {
        return "index";
    }

    @RequestMapping("/")
    public String getPeach(ModelMap map) {
        // 加入一个属性，用来在模板中读取
        map.addAttribute("host", "橙子");
        // return模板文件的名称，对应src/main/resources/templates/index.html
        return "index";
    }
    @RequestMapping("/js")
    public String getJS() {
        // return模板文件的名称，对应src/main/resources/templates/index.html
        //
        return "orange";
    }
}