package pl.polsl.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import pl.polsl.controller.ContractsController;

/**
 * Created by Aleksandra on 2016-04-07.
 */
@Controller
public class ContractWebController {
    @Autowired
    ContractsController contractsController;

    @RequestMapping(value ="/contracts", method = RequestMethod.GET)
    public String all(Model model){
        model.addAttribute("contracts", contractsController.findAllContracts());
        return "contracts";
    }
    @RequestMapping(value ="/contractsNewView", method = RequestMethod.GET)
    public String contractsNewView(Model model){
        return "newContract";
    }
    @RequestMapping(value ="/addContract", method = RequestMethod.POST)
    public String message(RedirectAttributes redirectAttributes, @RequestParam(value = "content", required = false)String content) {
        return "redirect:/contractsNewView/";
    }
}