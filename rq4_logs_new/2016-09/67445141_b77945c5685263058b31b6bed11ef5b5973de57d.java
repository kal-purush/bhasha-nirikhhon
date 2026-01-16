package com.Polodz.WebController;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by Łukasz on 07.09.2016.
 */

@Controller
public class MainPageController {

    @GetMapping("/")
    public ModelAndView getMainPage() {
        return new ModelAndView("mainPage");
    }
}