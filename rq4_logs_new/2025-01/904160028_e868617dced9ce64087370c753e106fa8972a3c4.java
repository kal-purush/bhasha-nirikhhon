package fr.eni.encheres.controller;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.error.ErrorAttributeOptions.Include;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.WebRequest;

import java.util.Map;

@Controller
public class CustomErrorController implements ErrorController {

    private final ErrorAttributes errorAttributes;

    public CustomErrorController(ErrorAttributes errorAttributes) {
        this.errorAttributes = errorAttributes;
    }

    @RequestMapping("/error")
    public String handleError(WebRequest webRequest, Model model) {
        // Créer des options pour inclure des détails d'erreur spécifiques
        ErrorAttributeOptions options = ErrorAttributeOptions.of(
                Include.MESSAGE, Include.EXCEPTION, Include.STACK_TRACE
        );

        // Récupération des attributs d'erreur
        Map<String, Object> errorDetails = errorAttributes.getErrorAttributes(
                webRequest, options
        );

        // Récupérer le code de statut et les détails
        int status = (int) errorDetails.getOrDefault("status", 500);
        model.addAttribute("status", status);
        model.addAttribute("error", errorDetails.get("error"));
        model.addAttribute("message", errorDetails.get("message"));

        // Rediriger vers une page d'erreur spécifique selon le statut
        if (status == 404) {
            return "error/404";
        } else if (status == 500) {
            return "error/500";
        } else {
            return "error/generic";
        }
    }
}