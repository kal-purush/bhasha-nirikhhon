package org.sevenasix.medium.registrar;

import org.sevenasix.medium.actors.Student;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EnrollmentController {

    @RequestMapping("/student/add")
    public Student createStudent(@RequestParam(value="name", defaultValue="Student") String name){

        return new Student(name);
    }


}