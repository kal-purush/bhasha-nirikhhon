package com.epam.brest.summer.courses2019.web_app;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Car controller.
 */
@Controller
public class CarController {

    /**
     * Goto cars list page.
     *
     * @return view name
     */
    @GetMapping(value = "/cars")
    public final String departments(Model model) {
        return "cars";
    }

    /**
     * Goto car page.
     *
     * @return view name
     */
    @GetMapping(value = "/car")
    public final String gotoAddDepartmentPage(Model model) {
        return "car";
    }
}