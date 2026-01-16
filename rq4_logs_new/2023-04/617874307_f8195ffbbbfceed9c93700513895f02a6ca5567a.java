package acme.features.student.activity;

import acme.entities.activity.Activity;
import acme.entities.lecture.Course;
import acme.framework.controllers.AbstractController;
import acme.roles.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;

@Controller
public class StudentActivityController extends AbstractController<Student, Activity> {

    // Internal state ---------------------------------------------------------

    @Autowired
    protected StudentActivityListService listService;
    @Autowired
    protected StudentActivityShowService showService;
    @Autowired
    protected StudentActivityCreateService createService;
    // Constructors -----------------------------------------------------------

    @PostConstruct
    protected void initialise() {
        super.addBasicCommand("list", this.listService);
        super.addBasicCommand("show", this.showService);
        super.addBasicCommand("create", this.createService);
    }



}