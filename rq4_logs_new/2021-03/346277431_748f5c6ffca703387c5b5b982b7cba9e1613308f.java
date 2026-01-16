package hr.tvz.keepthechange.controller;

import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;


/**
 * Custom controller for error page.
 */
@Controller
public class CustomErrorController implements ErrorController {

	/**
	 * Get mapping for custom error controller.
	 * @param model {@link Model}
	 * @return custom error page
	 */
	@GetMapping("error")
	public String errorSite(Model model) {
		return "errorPage";
	}

	@Override
	public String getErrorPath() {
		return "error";
	}

}