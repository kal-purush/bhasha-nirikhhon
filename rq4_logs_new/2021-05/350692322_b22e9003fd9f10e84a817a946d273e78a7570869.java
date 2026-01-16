package upeldev.com.github.upel3.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import upeldev.com.github.upel3.model.Course;
import upeldev.com.github.upel3.model.StudentGroup;
import upeldev.com.github.upel3.model.User;
import upeldev.com.github.upel3.services.CourseService;
import upeldev.com.github.upel3.services.StudentGroupService;
import upeldev.com.github.upel3.services.UserService;

import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.List;

@Controller
public class GroupController {
    private final UserService userService;
    private final CourseService courseService;
    private final StudentGroupService studentGroupService;

    @Autowired
    public GroupController(UserService userService, CourseService courseService, StudentGroupService studentGroupService) {
        this.userService = userService;
        this.courseService = courseService;
        this.studentGroupService = studentGroupService;
    }


    @RequestMapping(value = "/course_groups", method = RequestMethod.GET)
    public String groupsInCourseDisplay(
            Model model,
            Principal principal,
            HttpServletRequest request)
    {
        User currentUser = userService.findByEmail(principal.getName());
        model.addAttribute("user", currentUser);

        try {
            Long id = Long.parseLong(request.getParameter("id"));
            Course course = courseService.findCourseById(id);
            model.addAttribute("course", course);

            List<StudentGroup> groups = studentGroupService.findByCourse(course);
            model.addAttribute("groups", groups);
        }

        catch(NumberFormatException nfe) {
            String errorMsg = "Zapytanie GET /course?id=<course_id> otrzymało niewłaściwy typ danych. Spodziewany typ: long integer.";
            model.addAttribute("errorMsg", errorMsg);
            return "error";
        }
        return "course_groups";
    }
}