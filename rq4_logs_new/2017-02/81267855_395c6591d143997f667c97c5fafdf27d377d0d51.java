package helloPackage;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/lists_private")

/**
 * Created by Jonathan on 2/11/2017.
 */
public class PrivateToDoListsController {
    @RequestMapping(method = RequestMethod.GET)
    public String printHello(ModelMap model) {
        return "lists_private";//Name of the jsp
    }
}