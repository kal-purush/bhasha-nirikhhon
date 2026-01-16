package upeldev.com.github.upel3.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import upeldev.com.github.upel3.model.*;
import upeldev.com.github.upel3.services.*;

import java.security.Principal;
import java.util.List;

@Controller
public class ActivitySettingsController {
    private final UserService userService;
    private final CourseService courseService;
    private final ActivityService activityService;
    private final GradeService gradeService;
    private final SubGradeService subGradeService;

    @Autowired
    public ActivitySettingsController(UserService userService, CourseService courseService, ActivityService activityService, GradeService gradeService, SubGradeService subGradeService){
        this.userService = userService;
        this.courseService = courseService;
        this.activityService = activityService;
        this.gradeService = gradeService;
        this.subGradeService = subGradeService;
    }

    @RequestMapping(value = "/activity_settings/{courseId}/{activityId}", method = RequestMethod.GET)
    public String editGrades(@PathVariable("courseId") Long courseId,
                             @PathVariable("activityId") Long activityId,
                             Model model, Principal principal){
        User currentUser = userService.findByEmail(principal.getName());

        Course currentCourse = courseService.findCourseById(courseId);
        model.addAttribute("user", currentUser);

        Activity currentActivity = activityService.findActivityById(activityId);
        List<SubActivity> subActivities =  currentActivity.getSubActivities();

        try{
            if(!currentUser.getRoles().contains(Role.ADMIN) && !currentCourse.getLecturers().contains(currentUser)){
                String errorMsg = "Nie masz wystarczających uprawnień, aby edytować aktywność";
                model.addAttribute("errorMsg", errorMsg);
                return "redirect:/error";
            }
        }
        catch (IllegalArgumentException e){
            String errorMsg = "Podano nieprawidłowe argumenty podczas tworzenia kursu.";
            model.addAttribute("errorMsg", errorMsg);
            return "error";
        }
        model.addAttribute("course", currentCourse);
        model.addAttribute("activity", currentActivity);
        return "activity_settings";
    }

    @RequestMapping(value = "/activity_settings/description/{courseId}/{activityId}", method = RequestMethod.GET)
    public String editDescription(@PathVariable("courseId") Long courseId,
                                @PathVariable("activityId") Long activityId,
                                @RequestParam(value = "description") String description,
                                Model model, Principal principal) {
        User currentUser = userService.findByEmail(principal.getName());

        Course currentCourse = courseService.findCourseById(courseId);
        model.addAttribute("user", currentUser);

        Activity currentActivity = activityService.findActivityById(activityId);

        activityService.changeDescription(currentActivity, description);

        return String.format("redirect:/activity_settings/%d/%d", courseId, activityId);
    }

    @RequestMapping(value = "/activity_settings/passval/{courseId}/{activityId}", method = RequestMethod.GET)
    public String editPassValue(@PathVariable("courseId") Long courseId,
                                @PathVariable("activityId") Long activityId,
                                @RequestParam(value = "passValue") Integer passValue,
                                Model model, Principal principal) {
        User currentUser = userService.findByEmail(principal.getName());

        Course currentCourse = courseService.findCourseById(courseId);
        model.addAttribute("user", currentUser);

        Activity currentActivity = activityService.findActivityById(activityId);

        activityService.changePassValue(currentActivity, passValue);

        return String.format("redirect:/activity_settings/%d/%d", courseId, activityId);
    }
}