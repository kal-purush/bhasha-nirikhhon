package seu.controller;

import com.owlike.genson.Genson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import seu.service.AdminService;
import seu.service.StudentService;
import seu.service.TeacherService;

@Controller
public class OverViewController {

    @Autowired
    StudentService studentService;
    @Autowired
    TeacherService teacherService;
    @Autowired
    AdminService adminService;

    private Genson genson = new Genson();

    public String getOverView() {
        switch (MainController.role) {
            case 1:
                return genson.serialize(studentService.queryStudentByStudentId(MainController.account));
            case 2:
                return genson.serialize(teacherService.queryTeacherByTeacherId(MainController.account));
            case 3:
                return genson.serialize(adminService.queryAdminByAdminId(MainController.account));
            default:
                return null;
        }
    }
}