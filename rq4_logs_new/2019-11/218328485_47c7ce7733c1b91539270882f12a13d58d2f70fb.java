package com.project.teamup.controller;

import com.project.teamup.model.User;
import com.project.teamup.model.VerificationToken;
import com.project.teamup.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.MessageSource;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;

import java.util.Calendar;
import java.util.Locale;

@RestController
@RequestMapping(RegistrationController.BASE_URL)
public class RegistrationController {
    public static final String BASE_URL="/user/registration";

    @Autowired
    private UserService userService;

    private MessageSource messages;

    public RegistrationController(@Qualifier("messageSource") MessageSource messages) {
        this.messages = messages;
    }

    //@RequestParam("token")
    @RequestMapping(value = "/registrationConfirm/{token}", method = RequestMethod.GET)
    public String confirmRegistration(WebRequest request, Model model, @PathVariable String token) {

        Locale locale = Locale.US;

        VerificationToken verificationToken = userService.getVerificationToken(token);
        if (verificationToken == null) {
            String message = "Invalid token.";
            model.addAttribute("message", message);
            return "redirect:/badUser.html?lang=" + locale.getLanguage();
        }

        User user = verificationToken.getUser();
        Calendar cal = Calendar.getInstance();
        if ((verificationToken.getExpiryDate().getTime() - cal.getTime().getTime()) <= 0) {
            String messageValue = "Your registration token has expired. Please register again.";
            model.addAttribute("message", messageValue);
            return "redirect:/badUser.html?lang=" + locale.getLanguage();
        }

        //user.setEnabled(true);
        //userService.save(user);
        return "redirect:/login.html?lang=" + locale.getLanguage();
    }
}

/*        Locale locale = request.getLocale();

        VerificationToken verificationToken = userService.getVerificationToken(token);
        if (verificationToken == null) {
            String message = messages.getMessage("auth.message.invalidToken", null, locale);
            model.addAttribute("message", message);
            return "redirect:/badUser.html?lang=" + locale.getLanguage();
        }

        User user = verificationToken.getUser();
        Calendar cal = Calendar.getInstance();
        if ((verificationToken.getExpiryDate().getTime() - cal.getTime().getTime()) <= 0) {
            String messageValue = messages.getMessage("auth.message.expired", null, locale);
            model.addAttribute("message", messageValue);
            return "redirect:/badUser.html?lang=" + locale.getLanguage();
        }

        //user.setEnabled(true);
        userService.save(user);
        return "redirect:/login.html?lang=" + request.getLocale().getLanguage();*/