package seu.controller;

import com.owlike.genson.Genson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import seu.domain.Course;
import seu.service.CourseSelectService;
import seu.service.CourseService;

import java.util.ArrayList;
import java.util.List;

@Controller
public class CourseController {

    @Autowired
    CourseSelectService courseSelectService;
    @Autowired
    CourseService courseService;

    private Genson genson = new Genson();

    public String getAllCourse() {
        return genson.serialize(courseService.queryAll());
    }

    public String getSelectedCourse() {
        return genson.serialize(courseService.queryCourseByStudentId(MainController.account));
    }

    public String getNoSelectedCourse() {
        List<Course> all = courseService.queryAll();
        List<Course> selected = courseService.queryCourseByStudentId(MainController.account);
        List<Course> noSelected = new ArrayList<Course>();
        for (int i = 0; i < all.size(); i++) {
            boolean contain = false;
            for (int j = 0; j < selected.size(); j++) {
                if (all.get(i).getCourseId() == selected.get(j).getCourseId()) {
                    contain = true;
                }
            }
            if (!contain)
                noSelected.add(all.get(i));
        }
        return genson.serialize(noSelected);
    }

    public int addCourse(int id) {
        return courseSelectService.insertCourseSelect(MainController.account, id);
    }

    public int deleteCourse(int id) {
        return courseSelectService.deleteCourseSelectByCourseIDAndStudentID(MainController.account, id);
    }
}