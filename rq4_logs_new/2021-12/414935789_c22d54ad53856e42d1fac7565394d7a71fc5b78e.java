package com.kitchen.iChef.Controller;

import com.kitchen.iChef.Controller.Model.Response.UtensilResponse;
import com.kitchen.iChef.Mapper.UtensilMapper;
import com.kitchen.iChef.Service.UtensilService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/utensils")
public class UtensilController {
    private final UtensilService utensilService;
    private final UtensilMapper utensilMapper;

    public UtensilController(UtensilService utensilService) {
        this.utensilService = utensilService;
        this.utensilMapper = new UtensilMapper();
    }

    @GetMapping
    public List<UtensilResponse> getAllUtensils() {
        return utensilService.getAllUtensils()
                .stream()
                .map(utensilMapper::mapToResponse)
                .collect(Collectors.toList());
    }
}