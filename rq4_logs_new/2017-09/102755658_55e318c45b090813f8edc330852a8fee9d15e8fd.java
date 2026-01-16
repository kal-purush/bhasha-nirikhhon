package seu.controller;

import com.owlike.genson.Genson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import seu.service.AdminService;
import seu.service.ClassService;
import seu.service.StudentService;
import seu.service.TeacherService;

@Controller
public class StudentRollController {

    @Autowired
    StudentService studentService;
    @Autowired
    TeacherService teacherService;
    @Autowired
    AdminService adminService;
    @Autowired
    ClassService classService;

    private Genson genson = new Genson();

    public String getStudentById() {
        return genson.serialize(studentService.queryStudentByStudentId(MainController.account));
    }

    public String getClassById() {
        return genson.serialize(classService.queryClassByStudentId(MainController.account));
    }

    public String getTeacherById(int teacherId) {
        return genson.serialize(teacherService.queryTeacherByTeacherId(teacherId));
    }

    public String getClassByTeacherId() {
        return genson.serialize(classService.queryClassByTeacherId(MainController.account));
    }

    public String getStudentByClassId(int classId) {
        return genson.serialize(studentService.queryStudentByClassId(classId));
    }
}