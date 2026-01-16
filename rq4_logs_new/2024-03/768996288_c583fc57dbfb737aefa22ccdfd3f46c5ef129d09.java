package api.admin;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@RequestMapping("/api/admin")
@Controller
@ResponseBody
public class AdminController {
    //Admin page
    // 접근 권한 테스트 용 - 현재는 admin role 존재하지 않음
    @GetMapping("/test")
    public String adminTest(){
        return "admin test authorized";
    }
}