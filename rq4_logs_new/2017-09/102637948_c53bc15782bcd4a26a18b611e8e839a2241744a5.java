package pl.quayal.app.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.quayal.app.server.model.Course;
import pl.quayal.app.server.model.Trainer;
import pl.quayal.app.server.service.CourseService;

@RestController
@RequestMapping("/course")

public class CourseController {

    @Autowired
    private CourseService courseService;

    @PostMapping
    public Course addCourse(String courseName, Trainer mainTrainer, Trainer secondTrainer) {

        return courseService.addCourse(courseName, mainTrainer, secondTrainer);

    }

}

