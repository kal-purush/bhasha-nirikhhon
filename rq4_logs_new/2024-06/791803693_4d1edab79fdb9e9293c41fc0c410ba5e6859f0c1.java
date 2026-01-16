package ro.fithubhome.bodystats.config;

import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class RootErrorController implements ErrorController {
    @RequestMapping("error")
    private String handleError() {
        return "error/error.html";
    }
}