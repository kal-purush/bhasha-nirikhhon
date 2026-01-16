package org.detective.controller.Speciality;

import org.detective.entity.Speciality;
import org.detective.services.Speciality.SpecialityService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/speciality")
public class SpecialtyController {

    private final SpecialityService specialityService;

    public SpecialtyController(SpecialityService specialityService) {
        this.specialityService = specialityService;
    }
    @GetMapping
    public List<Speciality> getAllLocation() {
        System.out.println(specialityService.getAllSpecialties());
        return specialityService.getAllSpecialties();
    }
}