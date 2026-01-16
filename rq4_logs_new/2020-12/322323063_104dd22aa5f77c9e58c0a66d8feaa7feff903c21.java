package softuni.workshop.web.controllers;

import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.ModelAndView;
import softuni.workshop.error.EntityNotFoundException;

import java.util.List;

@ControllerAdvice
public class ErrorController {

    @ExceptionHandler
    public ModelAndView getEntityNotFoundException(EntityNotFoundException ex){
        return new ModelAndView("error", "errors", List.of(ex.getMessage()));
    }
}