package pl.cx;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 */
@Controller
public class ExampleController {
    @RequestMapping("/hello")
    public ModelAndView sayHello() {
        String message = "Welcome to Spring!!";
        return new ModelAndView("hello", "message", message);
    }
}