package jp.gr.java_conf.star_diopside.spark.web.controller;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.security.authentication.AccountExpiredException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.WebAttributes;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.BindingResult;
import org.springframework.validation.Errors;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import jp.gr.java_conf.star_diopside.spark.web.exception.DualLoginException;
import jp.gr.java_conf.star_diopside.spark.web.form.UserLoginForm;

@Controller
@RequestMapping("/authentication")
public class AuthenticationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationController.class);
    private static final Map<String, String> EXCEPTION_MESSAGES;

    @Inject
    private MessageSourceAccessor messages;

    static {
        HashMap<String, String> map = new HashMap<>();
        map.put(BadCredentialsException.class.getName(), "error.badCredentials");
        map.put(UsernameNotFoundException.class.getName(), "error.badCredentials");
        map.put(DisabledException.class.getName(), "error.accountDisabled");
        map.put(LockedException.class.getName(), "error.accountLocked");
        map.put(AccountExpiredException.class.getName(), "error.userInvalid");
        map.put(DualLoginException.class.getName(), "error.dualLogin");
        EXCEPTION_MESSAGES = Collections.unmodifiableMap(map);
    }

    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.setAllowedFields("username", "password");
    }

    @GetMapping("/create")
    public String create(UserLoginForm form, ModelMap model, Errors errors) {
        Exception exception = (Exception) model.get(WebAttributes.AUTHENTICATION_EXCEPTION);
        if (exception != null) {
            String errorCode = EXCEPTION_MESSAGES.get(exception.getClass().getName());
            if (errorCode != null) {
                errors.reject(errorCode);
            } else {
                LOGGER.debug(exception.getMessage(), exception);
                errors.reject("error.authentication");
            }
        }
        return "authentication/create";
    }

    @PostMapping("/failure")
    public String failure(HttpServletRequest request, RedirectAttributes attr) {
        Exception exception = (Exception) request.getAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);
        if (exception != null) {
            attr.addFlashAttribute(WebAttributes.AUTHENTICATION_EXCEPTION, exception);
        }
        return "redirect:/authentication/create";
    }

    @PostMapping
    public String save(@Valid UserLoginForm form, BindingResult result) {
        if (result.hasErrors()) {
            return "authentication/create";
        } else {
            return "forward:/login";
        }
    }

    @GetMapping("/logout")
    public String logout(RedirectAttributes attr) {
        attr.addFlashAttribute("message", messages.getMessage("info.logoutSuccess"));
        return "redirect:/authentication/create";
    }
}