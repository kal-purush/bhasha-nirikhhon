package studentenrollment.familyinfo.controller;

import studentenrollment.familyinfo.model.Parents;
import studentenrollment.familyinfo.service.ParentsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/family/parents")
public class ParentsController {
    private ParentsService parentsService;

    @Autowired
    public ParentsController(ParentsService parentsService) {
        this.parentsService = parentsService;
    }

    @PostMapping("/parents")
    public ResponseEntity<?> saveParents(@RequestBody Parents parents) {
        boolean studentExists = parentsService.checkIfStudentExists(parents.getStudentId());
        if (studentExists) {
            Parents savedParents = parentsService.saveParents(parents);
            return ResponseEntity.ok(savedParents);
        } else {

            return ResponseEntity.status(HttpStatus.OK).body("{\"message\": \"Student Id not found\"}");
        }
    }
}