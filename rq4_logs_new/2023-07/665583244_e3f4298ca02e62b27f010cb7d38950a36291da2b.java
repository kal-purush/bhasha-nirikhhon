package project.vilsoncake.BookOnlineStore.controller;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ExceptionController implements ErrorController {

    @RequestMapping("/error")
    public String handleError(HttpServletRequest request) {
        Object status = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);

        if (status != null) {
            final int statusCode = Integer.parseInt(status.toString());

            switch (statusCode) {
                case 403 -> {
                    return "error/403-status.html";
                }
                case 404 -> {
                    return "error/404-status.html";
                }
            }
        }
        return "error/unknown-error.html";
    }
}