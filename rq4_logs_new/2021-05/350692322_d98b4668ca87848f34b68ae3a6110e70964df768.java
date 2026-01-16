package upeldev.com.github.upel3.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import upeldev.com.github.upel3.model.*;
import upeldev.com.github.upel3.services.ActivityService;
import upeldev.com.github.upel3.services.CourseService;
import upeldev.com.github.upel3.services.GradeService;
import upeldev.com.github.upel3.services.UserService;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

@Controller
public class GradeSummaryController {
    private final CourseService courseService;
    private final UserService userService;
    private final GradeService gradeService;
    private final ActivityService activityService;
    @Autowired
    GradeSummaryController(CourseService courseService, UserService userService, GradeService gradeService, ActivityService activityService){
        this.courseService = courseService;
        this.userService = userService;
        this.gradeService = gradeService;
        this.activityService = activityService;
    }
    @RequestMapping(value = "/grade_summary")
    public String gradeSummary(@RequestParam(value = "id")  Long courseId, Model model, Principal principal){
        User currentUser = userService.findByEmail(principal.getName());
        model.addAttribute("user", currentUser);
        Course currentCourse = courseService.findCourseById(courseId);
        model.addAttribute("course", currentCourse);

        double userValue = 0;
        double valueMAX = 0;

        List<Activity> activityList = activityService.findActivityByCourse(currentCourse);
        List<Grade> gradeList = new ArrayList<>();

        for(Activity activity : activityList){
            gradeList.addAll(gradeService.findGradeByCourseAndUserAndActivity(currentCourse, currentUser, activity));
        }

        if(currentCourse.getAggregation().equals(ElementAggregation.SUM)){
            for(Grade grade : gradeList){
                userValue += grade.getValue();
                valueMAX += grade.getActivity().getValue();
            }

        }

        else { //TODO add agregation for ElementAggregation.WAVG
            for(Grade grade : gradeList){
                userValue += grade.getValue()/grade.getActivity().getValue();
            }
            valueMAX = 100;
            userValue = Math.round(userValue*100/gradeList.size());
        }

        int basePercentage;
        int bonusPercentage = 0;

        if(userValue < valueMAX){
            basePercentage = (int)(100.0 * userValue / valueMAX);
        }
        else{
            basePercentage = (int)(100.0 * valueMAX / userValue);
            bonusPercentage = 100 - basePercentage;
        }

        model.addAttribute("gradeList", gradeList);
        model.addAttribute("userValue", userValue);
        model.addAttribute("valueMAX", valueMAX);

        model.addAttribute("percentage", (int)(100.0 * userValue / valueMAX));
        model.addAttribute("basePercentage", basePercentage);
        model.addAttribute("bonusPercentage", bonusPercentage);

        String status;
        if(userValue < currentCourse.getPassValue()){
            model.addAttribute("barColor", "bg-danger");
            status = "Niezaliczone";
        }
        else{
            model.addAttribute("barColor", "bg-success");
            status = "Zaliczone";
        }
        model.addAttribute("status", status);

        return "grade_summary";
    }


}