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
public class EditGradeController {
    private final UserService userService;
    private final CourseService courseService;
    private final ActivityService activityService;
    private final GradeService gradeService;
    private final SubGradeService subGradeService;

    @Autowired
    public EditGradeController(UserService userService, CourseService courseService, ActivityService activityService, GradeService gradeService, SubGradeService subGradeService){
        this.userService = userService;
        this.courseService = courseService;
        this.activityService = activityService;
        this.gradeService = gradeService;
        this.subGradeService = subGradeService;
    }

    @RequestMapping(value = "/edit_grade/{courseId}/{activityId}/{userName}", method = RequestMethod.GET)
    public String editGrades(@PathVariable("courseId") Long courseId,
                               @PathVariable("activityId") Long activityId,
                               @PathVariable("userName")  String userName,
                               @RequestParam(value = "description")  String description,
                               @RequestParam("subGrade") double[] subActivityId,
                               Model model, Principal principal){
        User currentUser = userService.findByEmail(principal.getName());

        User modifiedUser = userService.findByEmail(userName);

        Course currentCourse = courseService.findCourseById(courseId);
        model.addAttribute("user", currentUser);

        Activity currentActivity = activityService.findActivityById(activityId);



        List<SubActivity> subActivities =  currentActivity.getSubActivities();

        try{
            if(!currentUser.getRoles().contains(Role.ADMIN) && !currentCourse.getLecturers().contains(currentUser)){
                String errorMsg = "Nie masz wystarczających uprawnień, aby utworzyć kurs";
                model.addAttribute("errorMsg", errorMsg);
                return "redirect:/error";
            }

            if(subActivities.size() != subActivityId.length){
                String errorMsg = "Nie wypełniono wszystkich wymaganych pól";
                model.addAttribute("errorMsg", errorMsg);
                return "redirect:/error";
            }

            if(gradeService.findGradeByCourseUserActivity(currentCourse, modifiedUser, currentActivity).size() == 0){
                Grade newGrade = new Grade(modifiedUser, currentActivity);
                newGrade.setDescription(description);
                gradeService.save(newGrade);


                for(int i = 0; i < subActivities.size(); i++){
                    SubGrade subGrade = new SubGrade(subActivities.get(i), newGrade, subActivityId[i]);
                    subGradeService.save(subGrade);
                }
            }
            else{
                Grade oldGrade = gradeService.findGradeByCourseUserActivity(currentCourse, modifiedUser, currentActivity).get(0);
                oldGrade.setDescription(description);
                for(int i = 0; i < subActivities.size(); i++){
                    SubGrade subGrade = oldGrade.getSubGrades().get(i);
                    subGrade.setValue(subActivityId[i]);
                }
                gradeService.save(oldGrade);

            }

        }
        catch (IllegalArgumentException e){
            String errorMsg = "Podano nieprawidłowe argumenty podczas tworzenia kursu.";
            model.addAttribute("errorMsg", errorMsg);
            return "error";
        }

        return "redirect:/activity?id="+activityId;
    }

    @RequestMapping(value = "/edit_grade/passval/{courseId}/{activityId}", method = RequestMethod.GET)
    public String editPassValue(@PathVariable("courseId") Long courseId,
                               @PathVariable("activityId") Long activityId,
                               @RequestParam(value = "passValue") Integer passValue,
                               Model model, Principal principal) {
        User currentUser = userService.findByEmail(principal.getName());

        Course currentCourse = courseService.findCourseById(courseId);
        model.addAttribute("user", currentUser);

        Activity currentActivity = activityService.findActivityById(activityId);

        activityService.changePassValue(currentActivity, passValue);

        return "redirect:/activity?id="+activityId;
    }
}