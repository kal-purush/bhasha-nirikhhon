package lk.ac.cmb.ucsc.scs3203.miniproject.backend.controller;

import lk.ac.cmb.ucsc.scs3203.miniproject.backend.models.Subject;
import lk.ac.cmb.ucsc.scs3203.miniproject.backend.service.SubjectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

@RestController
@RequestMapping("/api/v1/subject/")
public class SubjectController {

    SubjectService subjectService;

    public SubjectController(SubjectService subjectService) {
        this.subjectService = subjectService;
    }

    @GetMapping("/id/{id}")  //localhost:8080/scs3203/api/v1/subject/id/1
    String demo(@RequestBody Subject id){
        return "";
    }

}