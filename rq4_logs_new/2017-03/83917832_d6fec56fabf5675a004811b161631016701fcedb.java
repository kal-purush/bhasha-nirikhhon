package com.danigu.web.blab;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.inject.Inject;

/**
 * @author dani
 */
@Controller
@RequestMapping("/blab")
public class BlabController {

    @Inject
    private BlabRepository repository;

    @GetMapping("/{id}")
    public String getById(@PathVariable Long id, Model model) {
        Blab blab = repository.findOne(id);
        if(blab == null) return "404";

        model.addAttribute("blab", blab);
        return "blab";
    }

    @GetMapping
    public String lof() {
        return "index";
    }
}