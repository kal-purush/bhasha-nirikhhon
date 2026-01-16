package pl.polsl.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import pl.polsl.controller.ShowroomsController;

/**
 * Created by Dominika Błasiak on 08.04.2016.
 */
@Controller
public class ShowroomWebController {
    @Autowired
    private ShowroomsController showroomsController;
    /**
     * Load on page about groups
     * @param model
     * @return showroom.html
     */
    @RequestMapping(value ="/showroom")
    public String allGroups(Model model){
        model.addAttribute("showrooms", showroomsController.findAll());
        return "showroom";
    }
}