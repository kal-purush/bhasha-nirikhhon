package jm.stockx.controller;

import jm.stockx.UserService;
import jm.stockx.dto.UserDTO;
import jm.stockx.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.validation.Valid;

@Controller
@RequestMapping("/registration")
public class RegistrationController {
    private final UserService userService;

    @Autowired
    public RegistrationController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping
    public String registrationForm(Model model) {
        model.addAttribute("user", new UserDTO());
        //todo можно либо совместить регистрацию с логином в 1 форме (как на сайте) -> return "login"; либо нужно создать отдельный registration.html
        return "registration";
    }

    @PostMapping
    public String registerUser(@ModelAttribute("user") @Valid UserDTO userDto,
                                      BindingResult result, Model model){
        if (result.hasErrors()) {
            return "registration";
        }
        if (userService.getUserByEmail(userDto.getEmail()) != null) {
            model.addAttribute("errorMessage", "There is already an account associated with this email address. Please login using your existing account");
            return "registration";
        }
        userService.createUser(new User(userDto));
        return "redirect:/";
    }

}