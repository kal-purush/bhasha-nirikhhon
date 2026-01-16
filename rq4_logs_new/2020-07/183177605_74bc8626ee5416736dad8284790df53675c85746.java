package com.wagner.blueprint.web.mvc;

import com.wagner.blueprint.config.Endpoints;
import com.wagner.blueprint.config.ViewNames;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class ToDoController {

  @GetMapping(Endpoints.TODO_LIST)
  public ModelAndView showToDoList() {
    return new ModelAndView(ViewNames.TODO_LIST);
  }
}