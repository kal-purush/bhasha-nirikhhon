package upeldev.com.github.upel3.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import upeldev.com.github.upel3.model.User;
import upeldev.com.github.upel3.services.UserService;

import java.security.Principal;

@Controller
public class ChangePasswordController {

    private final UserService userService;

    @Autowired
    public ChangePasswordController(UserService userService) {
        this.userService = userService;
    }

    @RequestMapping(path="/change_password")
    public String newUser(Model model, Principal principal) {
        User currentUser = userService.findByEmail(principal.getName());
        model.addAttribute("user", currentUser);
        model.addAttribute("userService", userService);
        return "change_password";
    }

    @RequestMapping(value = "/change_password2")
    public String changePassword(
            @RequestParam(value = "oldPassword") String oldPassword,
            @RequestParam(value = "newPassword") String newPassword,
            @RequestParam(value = "repeatedNewPassword") String repeatedNewPassword,
            Model model,
            Principal principal) {

        User currentUser = userService.findByEmail(principal.getName());
        model.addAttribute("user", currentUser);

        try {
            if(!new BCryptPasswordEncoder().matches(oldPassword, currentUser.getPassword())) {
                throw new IllegalArgumentException("Podano nieprawidłowe stare hasło");
            }

            if(!newPassword.equals(repeatedNewPassword)) {
                throw new IllegalArgumentException("Nowe hasła nie są takie same");
            }

            //currentUser.setPassword(new BCryptPasswordEncoder().encode(newPassword));
            userService.changePassword(currentUser, newPassword);

        } catch (IllegalArgumentException e) {
            String errorMsg = e.getMessage();
            model.addAttribute("errorMsg", errorMsg);
            return "error";
        }
        return "redirect:/profile";
    }
}