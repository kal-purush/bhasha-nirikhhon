package upeldev.com.github.upel3.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import upeldev.com.github.upel3.model.User;
import upeldev.com.github.upel3.services.CourseService;
import upeldev.com.github.upel3.services.UserService;

import java.security.Principal;

@Controller
public class CustomErrorController implements ErrorController {
    private final UserService userService;
    private final CourseService courseService;

    @Autowired
    public CustomErrorController(UserService userService, CourseService courseService){
        this.userService = userService;
        this.courseService = courseService;
    }

    @RequestMapping(value = "/error")
    public String errorPage(Model model, Principal principal) {
        User currentUser = userService.findByEmail(principal.getName());
        model.addAttribute("user", currentUser);

        String errorMsg = "Nieznany błąd";
        model.addAttribute("errorMsg", errorMsg);
        return "error";
    }

    @Override
    public String getErrorPath() {
        return null;
    }
}