package com.Polodz.webController;

import com.Polodz.service.WebService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by lpolatowski on 2016-09-07.
 */

@Controller
public class WebMovieListController {

	@Autowired
	private WebService service;

	@GetMapping("/movieList")
	public ModelAndView getMovieList() {
		return new ModelAndView("movieList").addObject("string", service.getMovieListString());
	}

}