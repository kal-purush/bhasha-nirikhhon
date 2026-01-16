package org.cplier.codegen.controller;

import lombok.extern.slf4j.Slf4j;
import org.cplier.codegen.entity.MicroService;
import org.cplier.codegen.service.MicroServiceService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.Resource;
import javax.validation.Valid;

@Slf4j
@Controller
public class MicroServiceController {

  @Resource MicroServiceService microServiceService;

  @PostMapping("/addservice")
  public String addService(@Valid MicroService microService, BindingResult result, Model model) {
    if (result.hasErrors()) {
      return "add-service";
    }

    MicroService update = microServiceService.save(microService);
    log.debug("the service={} has been created", update.getId());
    model.addAttribute("services", microServiceService.findAll());
    return "index";
  }

  @PostMapping("/update/{id}")
  public String updateService(
      @PathVariable("id") long id,
      @Valid MicroService microService,
      BindingResult result,
      Model model) {
    if (result.hasErrors()) {
      microService.setId(id);
      return "update-service";
    }

    microServiceService.save(microService);
    model.addAttribute("services", microServiceService.findAll());
    return "index";
  }

  @GetMapping("/edit/{id}")
  public String editService(@PathVariable("id") long id, Model model) {
    MicroService edit =
        microServiceService
            .findById(id)
            .orElseThrow(() -> new IllegalArgumentException("Invalid service Id:" + id));
    model.addAttribute("service", edit);
    return "edit-service";
  }

  @GetMapping("/delete/{id}")
  public String deleteService(@PathVariable("id") long id, Model model) {
    MicroService microService =
        microServiceService
            .findById(id)
            .orElseThrow(() -> new IllegalArgumentException("Invalid service Id:" + id));
    microServiceService.delete(microService);
    model.addAttribute("services", microServiceService.findAll());
    return "index";
  }
}