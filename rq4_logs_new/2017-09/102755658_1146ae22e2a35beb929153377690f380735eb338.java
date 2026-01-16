package seu.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import seu.service.AdminService;
import seu.service.StudentService;
import seu.service.TeacherService;

@Controller
public class LoginController {

    @Autowired
    private StudentService studentService;
    @Autowired
    private TeacherService teacherService;
    @Autowired
    private AdminService adminService;

    public int login(String account, String password, int role) {
        switch (role) {
            case 1:
                return studentService.login(Integer.parseInt(account), password);
            case 2:
                return teacherService.login(Integer.parseInt(account), password);
            case 3:
                return adminService.login(Integer.parseInt(account), password);
            default:
                return 0;
        }
    }
}