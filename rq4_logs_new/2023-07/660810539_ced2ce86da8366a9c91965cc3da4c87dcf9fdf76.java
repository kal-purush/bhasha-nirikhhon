package studentenrollment.Academicinfo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import studentenrollment.Academicinfo.model.Academy;
import studentenrollment.Academicinfo.service.AcademyService;

@RestController
@RequestMapping("/academies")
public class AcademyController {
    private final AcademyService academyService;

    @Autowired
    public AcademyController(AcademyService academyService) {
        this.academyService = academyService;
    }

    @PostMapping
    public ResponseEntity<Academy> createAcademy(@RequestBody Academy academy) {
        try {
            Academy savedAcademy = academyService.addNewSchool(academy);
            return ResponseEntity.status(HttpStatus.CREATED).body(savedAcademy);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }
}




























