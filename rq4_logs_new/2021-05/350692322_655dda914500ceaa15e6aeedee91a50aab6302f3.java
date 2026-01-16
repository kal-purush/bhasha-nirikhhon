package upeldev.com.github.upel3.controllers;

import org.apache.catalina.connector.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import upeldev.com.github.upel3.model.Course;
import upeldev.com.github.upel3.model.User;
import upeldev.com.github.upel3.model.achievement.Achievement;
import upeldev.com.github.upel3.services.AchievementService;
import upeldev.com.github.upel3.services.CourseService;
import upeldev.com.github.upel3.services.StudentAchievementService;
import upeldev.com.github.upel3.services.UserService;

import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.List;

@Controller
public class AchievementsController {


    private final AchievementService achievementService;
    private final StudentAchievementService studentAchievementService;
    private final CourseService courseService;
    private final UserService userService;

    @Autowired
    public AchievementsController(AchievementService achievementService,
                                  StudentAchievementService studentAchievementService,
                                  CourseService courseService,
                                  UserService userService){
        this.achievementService = achievementService;
        this.studentAchievementService= studentAchievementService;
        this.courseService = courseService;
        this.userService = userService;
    }


    @RequestMapping(value = "/achievements", method = RequestMethod.GET)
    public String showAchievements(Model model,
                                   Principal principal,
                                   HttpServletRequest request){

        Course course = courseService.findCourseById(Long.parseLong(request.getParameter("courseId")));
        User currentUser = userService.findByEmail(principal.getName());
        model.addAttribute("user", currentUser);

        List<Achievement> achievements = achievementService.findAllByCourse(course);

        model.addAttribute("achievements", achievements);

        if(course.getLecturers().contains(currentUser)) return "achievements_lecturer";
        else return "error";

    }

}