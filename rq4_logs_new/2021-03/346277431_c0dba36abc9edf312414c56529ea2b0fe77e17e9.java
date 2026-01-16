package hr.tvz.keepthechange.controller;

import hr.tvz.keepthechange.dto.UserRegistrationDto;
import hr.tvz.keepthechange.entity.User;
import hr.tvz.keepthechange.service.UserService;
import hr.tvz.keepthechange.validator.UserRegistrationValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;

/**
 * Contains mappings related to user registration.
 */
@Controller
@RequestMapping("/registration")
public class RegistrationController {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegistrationController.class);
    public static final String USER_REGISTRATION_DTO = "userRegistrationDto";
    private final UserService userService;
    private final UserRegistrationValidator userRegistrationValidator;

    public RegistrationController(UserService userService,
                                  UserRegistrationValidator userRegistrationValidator) {
        this.userService = userService;
        this.userRegistrationValidator = userRegistrationValidator;
    }

    /**
     * Adds a {@link UserRegistrationValidator} validator bean to the web data binder for model attribute with name {@link #USER_REGISTRATION_DTO}
     */
    @InitBinder(USER_REGISTRATION_DTO)
    protected void initBinder(WebDataBinder binder) {
        binder.addValidators(userRegistrationValidator);
    }

    @GetMapping
    public String showRegistrationPage(Model model) {
        model.addAttribute(USER_REGISTRATION_DTO, new UserRegistrationDto());
        return "registration";
    }

    /**
     * Registers a new user with the given form data and redirects to login screen, if there are no validation errors.
     *
     * @param userRegistrationDto user registration form data
     * @param bindingResult       binding result and errors
     * @return view name
     */
    @PostMapping
    public String performUserRegistration(@Validated @ModelAttribute(USER_REGISTRATION_DTO) UserRegistrationDto userRegistrationDto,
                                          BindingResult bindingResult) {
        if (bindingResult.hasErrors()) {
            LOGGER.debug("User registration form has errors");
            return "registration";
        }
        User newUser = userService.registerNewUser(userRegistrationDto);
        LOGGER.debug("Newly inserted user: {}", newUser);
        return "redirect:/login";
    }
}