package seu.controller;

import com.owlike.genson.Genson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import seu.service.DormitoryService;
import seu.service.StudentService;

@Controller
public class dormitoryController {

    @Autowired
    DormitoryService dormitoryService;

    @Autowired
    StudentService studentService;

    private Genson genson = new Genson();

    public String getAllDormitory() {
        return genson.serialize(dormitoryService.getDormitoryAll());
    }

    public String getStudentByDormitoryId(int id) {
        return genson.serialize(studentService.queryStudentByDormitoryId(id));
    }
}