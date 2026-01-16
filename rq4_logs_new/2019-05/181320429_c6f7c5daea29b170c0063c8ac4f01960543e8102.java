package org.plan.managementweb.web;

import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import javax.servlet.http.HttpServletRequest;

@Controller
public class WebErrorController implements ErrorController{

    @RequestMapping("/error")
    public String handleError (HttpServletRequest request) {
        return "/index.html";
    }

    @Override
    public String getErrorPath () {
        return "/error";
    }
}